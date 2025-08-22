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
        self.nodes = {}
        self.lock = threading.Lock()
        self.running = False
        self.heartbeat_timeout = 5
        self.stored_files = {}  # dict[str, FileInfo]
        self.pending_replications = defaultdict(list)  # dict[str, list[FileInfo]]

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
            if file_id in self.controller.stored_files:
                return storage_pb2.Status(message='File already exists', success=False)
            self.controller.stored_files[file_id] = file
            print(f"[Network] Uploaded file {file_id}")
            for n_id in list(self.controller.nodes.keys()):
                if self.controller.nodes[n_id]['status'] == 'active':
                    self.controller.pending_replications[n_id].append(file)
        return storage_pb2.Status(message='OK', success=True)

    def Download(self, request, context):
        file_id = request.file_id
        with self.controller.lock:
            if file_id in self.controller.stored_files:
                return storage_pb2.DownloadResponse(file=self.controller.stored_files[file_id], error='')
            else:
                return storage_pb2.DownloadResponse(file=None, error='File not found')

    def ListFiles(self, request, context):
        with self.controller.lock:
            files = [storage_pb2.FileSummary(file_id=k, file_name=v.file_name, total_size=v.total_size) for k, v in self.controller.stored_files.items()]
        return storage_pb2.ListFilesResponse(files=files)

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