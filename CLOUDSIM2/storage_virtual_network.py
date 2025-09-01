import time
import threading
from collections import defaultdict
import grpc
from concurrent import futures
import storage_pb2
import storage_pb2_grpc
from fastapi import FastAPI, HTTPException
import uvicorn
import socket

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
            target_file_id = None
            for fid in self.controller.file_metadata:
                if fid.endswith(f":{user_file_id}") or fid == user_file_id:
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
    def __init__(self, host: str = '0.0.0.0', port: int = 5000, api_port: int = 8000):
        self.controller = NetworkController(host, port)
        self.controller.start()
        self.heartbeat_checker = threading.Thread(
            target=self._check_heartbeats,
            daemon=True
        )
        self.heartbeat_checker.start()
        self.api_port = self._find_available_api_port(api_port)
        # Use 'localhost' instead of host to connect to the controller's gRPC server
        self.channel = grpc.insecure_channel(f'localhost:{port}')
        self.stub = storage_pb2_grpc.StorageServiceStub(self.channel)
        self._start_api()

    def _check_heartbeats(self):
        while self.controller.running:
            self.controller.check_node_status()
            time.sleep(1)

    def _find_available_api_port(self, base_port: int) -> int:
        port = base_port
        while True:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex(('localhost', port)) != 0:
                    return port
                port += 1
                if port > base_port + 2:  # Try up to 8002
                    raise RuntimeError("No available ports for API (8000-8002 in use)")

    def _start_api(self):
        app = FastAPI()

        @app.post("/upload")
        def upload(body: dict):
            required_keys = ['node_id', 'file_id', 'file_name', 'size_mb']
            if not all(key in body for key in required_keys):
                raise HTTPException(status_code=400, detail="Missing required fields: node_id, file_id, file_name, size_mb")
            try:
                # Simulate FileInfo (since API doesn't have actual chunks)
                file_id = f"{body['node_id']}:{body['file_id']}"
                size = int(body['size_mb'] * 1024 ** 2)
                chunks = []  # Simplified, as chunks are generated on node side; API assumes node handles
                file_info = storage_pb2.FileInfo(
                    file_id=file_id,
                    file_name=body['file_name'],
                    total_size=size,
                    chunks=chunks,
                    upload_timestamp=time.time()
                )
                response = self.stub.Upload(storage_pb2.UploadRequest(file=file_info))
                if not response.success:
                    raise HTTPException(status_code=400, detail=response.message)
                return {"status": "success", "message": "File uploaded and replicated"}
            except grpc.RpcError as e:
                raise HTTPException(status_code=500, detail=f"gRPC error: {str(e)}")

        @app.get("/download/{file_id}")
        def download(file_id: str):
            try:
                response = self.stub.Download(storage_pb2.DownloadRequest(file_id=file_id))
                if response.error:
                    raise HTTPException(status_code=404, detail=response.error)
                return {
                    "file_id": response.file.file_id,
                    "file_name": response.file.file_name,
                    "total_size_mb": response.file.total_size / (1024 ** 2),
                    "upload_timestamp": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(response.file.upload_timestamp))
                }
            except grpc.RpcError as e:
                raise HTTPException(status_code=500, detail=f"gRPC error: {str(e)}")

        @app.get("/list/{node_id}")
        def list_files(node_id: str):
            with self.controller.lock:
                if node_id not in self.controller.nodes or self.controller.nodes[node_id]['status'] != 'active':
                    raise HTTPException(status_code=404, detail=f"Node {node_id} not found or offline")
                channel_str = f"{self.controller.nodes[node_id]['host']}:{self.controller.nodes[node_id]['port']}"
            try:
                with grpc.insecure_channel(channel_str) as channel:
                    stub = storage_pb2_grpc.NodeServiceStub(channel)
                    response = stub.ListFiles(storage_pb2.ListFilesRequest())
                files = []
                for f in response.files:
                    files.append({
                        "file_id": f.file_id.split(':', 1)[1] if ':' in f.file_id else f.file_id,
                        "file_name": f.file_name,
                        "total_size_mb": f.total_size / (1024 ** 2),
                        "upload_timestamp": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(f.upload_timestamp))
                    })
                return {"files": files}
            except grpc.RpcError as e:
                raise HTTPException(status_code=500, detail=f"gRPC error fetching list from node {node_id}: {str(e)}")

        @app.get("/nodes")
        def get_nodes():
            with self.controller.lock:
                nodes_list = []
                for node_id, info in self.controller.nodes.items():
                    nodes_list.append({
                        "node_id": node_id,
                        "status": info['status'],
                        "host": info['host'],
                        "port": info['port'],
                        "last_seen": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(info['last_seen']))
                    })
                return {"nodes": nodes_list}

        @app.get("/storage/{node_id}")
        def get_storage_metrics(node_id: str):
            with self.controller.lock:
                if node_id not in self.controller.nodes or self.controller.nodes[node_id]['status'] != 'active':
                    raise HTTPException(status_code=404, detail=f"Node {node_id} not found or offline")
                channel_str = f"{self.controller.nodes[node_id]['host']}:{self.controller.nodes[node_id]['port']}"
            try:
                with grpc.insecure_channel(channel_str) as channel:
                    stub = storage_pb2_grpc.NodeServiceStub(channel)
                    response = stub.GetStorageMetrics(storage_pb2.GetStorageMetricsRequest())
                used_gb = response.used_storage / (1024 ** 3)
                total_gb = response.total_storage / (1024 ** 3)
                used_mb = response.used_storage / (1024 ** 2)
                total_mb = response.total_storage / (1024 ** 2)
                return {
                    "used_gb": used_gb,
                    "total_gb": total_gb,
                    "used_mb": used_mb,
                    "total_mb": total_mb,
                    "utilization_percent": (response.used_storage / response.total_storage * 100) if response.total_storage > 0 else 0
                }
            except grpc.RpcError as e:
                raise HTTPException(status_code=500, detail=f"gRPC error fetching storage metrics from node {node_id}: {str(e)}")

        def run_api():
            uvicorn.run(app, host="0.0.0.0", port=self.api_port)

        threading.Thread(target=run_api, daemon=True).start()
        print(f"[API] FastAPI started on port {self.api_port}")

    def shutdown(self):
        print("[Network] Shutting down controller...")
        self.controller.stop()
        self.controller.join()
        self.channel.close()  # Close the gRPC client channel
        print("[Network] Controller shutdown complete")