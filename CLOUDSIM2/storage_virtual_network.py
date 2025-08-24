import time
import threading
from collections import defaultdict
import grpc
from concurrent import futures
import storage_pb2
import storage_pb2_grpc

class NetworkController(threading.Thread):
    def __init__(self, host: str = '0.0.0.0', port: int = 5000):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.nodes = {}  # node_id -> {host, port, capacity, last_seen, status}
        self.file_metadata = {}  # file_id -> {name, size, upload_timestamp, replica_nodes}
        self.lock = threading.Lock()
        self.running = False
        self.heartbeat_timeout = 5
        self.pending_replications = defaultdict(list)  # node_id -> list[FileInfo]

    def run(self):
        self.running = True
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        storage_pb2_grpc.add_StorageServiceServicer_to_server(StorageServicer(self), server)
        server.add_insecure_port(f'{self.host}:{self.port}')
        server.start()
        print(f"[Network] Controller started on {self.host}:{self.port}")
        try:
            while self.running:
                time.sleep(1)
        finally:
            server.stop(None)

    def check_node_status(self):
        """Check which nodes are offline"""
        current_time = time.time()
        offline_nodes = []
        with self.lock:
            for node_id, info in list(self.nodes.items()):
                if info['status'] == 'registered':
                    continue
                if current_time - info['last_seen'] > self.heartbeat_timeout:
                    offline_nodes.append(node_id)
                    print(f"[Network] Node {node_id} went OFFLINE")
                    del self.nodes[node_id]
                    if node_id in self.pending_replications:
                        del self.pending_replications[node_id]
        return offline_nodes

    def stop(self):
        self.running = False

class StorageServicer(storage_pb2_grpc.StorageServiceServicer):
    def __init__(self, controller):
        self.controller = controller

    def Register(self, request, context):
        node_id = request.node_id
        with self.controller.lock:
            if node_id not in self.controller.nodes:
                print(f"[Network] Node {node_id} registered (came ONLINE)")
            self.controller.nodes[node_id] = {
                'host': request.host,
                'port': request.port,
                'capacity': dict(request.capacity),
                'last_seen': 0,
                'status': 'registered'
            }
        return storage_pb2.Status(message='OK', success=True)

    def ActiveNotification(self, request, context):
        node_id = request.node_id
        with self.controller.lock:
            if node_id in self.controller.nodes:
                if self.controller.nodes[node_id]['status'] != 'active':
                    print(f"[Network] Node {node_id} is now ACTIVE")
                self.controller.nodes[node_id]['status'] = 'active'
                self.controller.nodes[node_id]['last_seen'] = time.time()
                return storage_pb2.Status(message='ACK', success=True)
        return storage_pb2.Status(message='Node not registered', success=False)

    def Heartbeat(self, request, context):
        node_id = request.node_id
        with self.controller.lock:
            if node_id in self.controller.nodes:
                info = self.controller.nodes[node_id]
                if info['status'] == 'registered':
                    print(f"[Network] Node {node_id} is now ACTIVE")
                    info['status'] = 'active'
                info['last_seen'] = time.time()
                pending = self.controller.pending_replications[node_id]
                del self.controller.pending_replications[node_id]
                return storage_pb2.HeartbeatResponse(status='ACK', pending_replications=pending)
            else:
                return storage_pb2.HeartbeatResponse(status='ERROR')

    def Upload(self, request, context):
        file = request.file
        file_id = file.file_id
        with self.controller.lock:
            if file_id in self.controller.file_metadata:
                return storage_pb2.Status(message='File already exists', success=False)
            self.controller.file_metadata[file_id] = {
                'name': file.file_name,
                'size': file.total_size,
                'upload_timestamp': file.upload_timestamp,
                'replica_nodes': [n for n in self.controller.nodes if n != file_id.split(':')[0] and self.controller.nodes[n]['status'] == 'active']
            }
            print(f"[Network] Uploaded file {file_id} from {file_id.split(':')[0]}, replicating to {self.controller.file_metadata[file_id]['replica_nodes']}")
            for n_id in self.controller.file_metadata[file_id]['replica_nodes']:
                self.controller.pending_replications[n_id].append(file)
        return storage_pb2.Status(message='OK', success=True)

    def Download(self, request, context):
        user_file_id = request.file_id
        with self.controller.lock:
            # Search for file_id matching the user-provided id (suffix after ':')
            target_file_id = None
            for fid in self.controller.file_metadata:
                if fid.endswith(f":{user_file_id}") or fid == user_file_id:  # Match suffix or exact id
                    target_file_id = fid
                    break
            if not target_file_id:
                print(f"[Network] Download failed: {user_file_id} not in file_metadata")
                return storage_pb2.DownloadResponse(file=None, error='File not found')
            
            uploader_id = target_file_id.split(':')[0]
            candidates = [uploader_id] + self.controller.file_metadata[target_file_id]['replica_nodes']
            for node_id in candidates:
                if node_id in self.controller.nodes and self.controller.nodes[node_id]['status'] == 'active':
                    try:
                        channel_str = f"{self.controller.nodes[node_id]['host']}:{self.controller.nodes[node_id]['port']}"
                        print(f"[Network] Attempting to fetch {target_file_id} from {node_id} at {channel_str}")
                        with grpc.insecure_channel(channel_str) as channel:
                            stub = storage_pb2_grpc.NodeServiceStub(channel)
                            response = stub.GetFile(storage_pb2.DownloadRequest(file_id=target_file_id))
                            if response.error:
                                print(f"[Network] GetFile from {node_id} failed: {response.error}")
                                continue
                            print(f"[Network] Successfully fetched {target_file_id} from {node_id}")
                            return storage_pb2.DownloadResponse(file=response.file, error='')
                    except grpc.RpcError as e:
                        print(f"[Network] gRPC error fetching {target_file_id} from {node_id}: {e}")
                        continue
            print(f"[Network] Download failed: No active nodes with {target_file_id}")
            return storage_pb2.DownloadResponse(file=None, error='File not found')

class StorageVirtualNetwork:
    def __init__(self, host: str = '0.0.0.0', port: int = 5000):
        self.controller = NetworkController(host, port)
        self.controller.start()
        self.heartbeat_checker = threading.Thread(
            target=self._check_heartbeats,
            daemon=True
        )
        self.heartbeat_checker.start()

    def _check_heartbeats(self):
        while self.controller.running:
            self.controller.check_node_status()
            time.sleep(1)

    def shutdown(self):
        print("[Network] Shutting down controller...")
        self.controller.stop()
        self.controller.join()
        print("[Network] Controller shutdown complete")