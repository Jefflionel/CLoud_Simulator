import time
import math
import threading
import random
import hashlib
import grpc
import storage_pb2
import storage_pb2_grpc
from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from enum import Enum, auto

class TransferStatus(Enum):
    PENDING = auto()
    IN_PROGRESS = auto()
    COMPLETED = auto()
    FAILED = auto()

@dataclass
class FileChunk:
    chunk_id: int
    size: int  # in bytes
    checksum: str
    status: TransferStatus = TransferStatus.PENDING
    stored_node: Optional[str] = None

@dataclass
class FileTransfer:
    file_id: str
    file_name: str
    total_size: int  # in bytes
    chunks: List[FileChunk]
    status: TransferStatus = TransferStatus.PENDING
    created_at: float = time.time()
    completed_at: Optional[float] = None

class HeartbeatSender(threading.Thread):
    def __init__(self, node_id: str, stub, interval: float = 2):
        super().__init__(daemon=True)
        self.node_id = node_id
        self.stub = stub
        self.interval = interval
        self.running = True

    def run(self):
        while self.running:
            try:
                response = self.stub.Heartbeat(storage_pb2.HeartbeatRequest(node_id=self.node_id))
                if response.status == 'ACK':
                    for file in response.pending_replications:
                        self.parent.store_replicated_file(file)
                else:
                    print(f"[Node {self.node_id}] Heartbeat failed: {response.status}")
            except grpc.RpcError as e:
                print(f"[Node {self.node_id}] Heartbeat error: {e}")
            time.sleep(self.interval)

    def stop(self):
        self.running = False

class StorageVirtualNode:
    def __init__(
        self,
        node_id: str,
        cpu_capacity: int,
        memory_capacity: int,
        storage_capacity: int,
        bandwidth: float,  # Now in MB/s
        network_host: str = 'localhost',
        network_port: int = 5000
    ):
        self.node_id = node_id
        self.cpu_capacity = cpu_capacity
        self.memory_capacity = memory_capacity
        self.total_storage = storage_capacity * 1024 ** 3  # GB to bytes
        self.bandwidth = bandwidth * 1024 ** 2  # MB/s to bytes/s
        self.network_host = network_host
        self.network_port = network_port

        # Resource tracking
        self.used_storage = 0
        self.stored_files = {}  # dict[str, FileTransfer]

        # gRPC setup
        self.channel = grpc.insecure_channel(f'{self.network_host}:{self.network_port}')
        self.stub = storage_pb2_grpc.StorageServiceStub(self.channel)

        # Start heartbeat sender
        self.heartbeat_sender = HeartbeatSender(node_id=node_id, stub=self.stub)
        self.heartbeat_sender.parent = self
        self.heartbeat_sender.start()

        # Register with network
        self._register_with_network()
        self._notify_active()

    def _register_with_network(self):
        capacity = {
            'cpu': self.cpu_capacity,
            'memory': self.memory_capacity * 1024 ** 3,
            'storage': self.total_storage,
            'bandwidth': int(self.bandwidth)  # Convert to int for gRPC
        }
        request = storage_pb2.NodeInfo(node_id=self.node_id, host='localhost', port=0, capacity=capacity)
        try:
            response = self.stub.Register(request)
            if not response.success:
                raise RuntimeError(f"Registration failed: {response.message}")
            print(f"[Node {self.node_id}] Registered successfully")
        except grpc.RpcError as e:
            raise RuntimeError(f"Failed to register with network: {e}")

    def _notify_active(self):
        try:
            response = self.stub.ActiveNotification(storage_pb2.HeartbeatRequest(node_id=self.node_id))
            if not response.success:
                print(f"[Node {self.node_id}] Active notification failed: {response.message}")
        except grpc.RpcError as e:
            print(f"[Node {self.node_id}] Active notification error: {e}")

    def _calculate_chunk_size(self, file_size: int) -> int:
        if file_size < 10 * 1024 * 1024:  # < 10MB
            return 512 * 1024  # 512KB chunks
        elif file_size < 100 * 1024 * 1024:  # < 100MB
            return 2 * 1024 * 1024  # 2MB chunks
        else:
            return 10 * 1024 * 1024  # 10MB chunks

    def _generate_chunks(self, file_id: str, file_size: int) -> List[FileChunk]:
        chunk_size = self._calculate_chunk_size(file_size)
        num_chunks = math.ceil(file_size / chunk_size)
        chunks = []
        for i in range(num_chunks):
            fake_checksum = hashlib.md5(f"{file_id}-{i}".encode()).hexdigest()
            actual_chunk_size = min(chunk_size, file_size - i * chunk_size)
            chunks.append(FileChunk(
                chunk_id=i,
                size=actual_chunk_size,
                checksum=fake_checksum
            ))
        return chunks

    def store_replicated_file(self, file_info: storage_pb2.FileInfo):
        size = file_info.total_size
        if self.used_storage + size > self.total_storage:
            print(f"[Node {self.node_id}] Storage full, ignoring replication for {file_info.file_id}")
            return
        transfer = FileTransfer(
            file_id=file_info.file_id,
            file_name=file_info.file_name,
            total_size=size,
            chunks=[FileChunk(
                chunk_id=c.chunk_id,
                size=c.size,
                checksum=c.checksum,
                status=TransferStatus.COMPLETED,
                stored_node=self.node_id
            ) for c in file_info.chunks],
            status=TransferStatus.COMPLETED,
            completed_at=time.time()
        )
        self.stored_files[file_info.file_id] = transfer
        self.used_storage += size
        # print(f"[Node {self.node_id}] Received replication for file {file_info.file_id}")

    def upload_file(self, file_id: str, file_name: str, size_mb: float):
        size = int(size_mb * 1024 ** 2)  # Convert MB to bytes
        if self.used_storage + size > self.total_storage:
            print(f"[Node {self.node_id}] Insufficient storage for upload")
            return
        chunks = self._generate_chunks(file_id, size)
        file_info = storage_pb2.FileInfo(
            file_id=file_id,
            file_name=file_name,
            total_size=size,
            chunks=[storage_pb2.FileChunk(chunk_id=c.chunk_id, size=c.size, checksum=c.checksum) for c in chunks]
        )
        transfer_time = size_mb / (self.bandwidth / (1024 ** 2)) if self.bandwidth > 0 else 0  # MB / (MB/s) = seconds
        print(f"[Node {self.node_id}] Simulating upload transfer...")
        time.sleep(transfer_time)
        try:
            response = self.stub.Upload(storage_pb2.UploadRequest(file=file_info))
            if response.success:
                self.store_replicated_file(file_info)
                print(f"[Node {self.node_id}] Upload successful. Time taken: {transfer_time:.2f} seconds")
            else:
                print(f"[Node {self.node_id}] Upload failed: {response.message}")
        except grpc.RpcError as e:
            print(f"[Node {self.node_id}] Upload error: {e}")

    def download_file(self, file_id: str):
        try:
            response = self.stub.Download(storage_pb2.DownloadRequest(file_id=file_id))
            if response.error:
                print(f"[Node {self.node_id}] Download failed: {response.error}")
                return
            file = response.file
            size = file.total_size
            size_mb = size / (1024 ** 2)  # Convert bytes to MB
            if self.used_storage + size > self.total_storage:
                print(f"[Node {self.node_id}] Insufficient storage for download")
                return
            transfer_time = size_mb / (self.bandwidth / (1024 ** 2)) if self.bandwidth > 0 else 0  # MB / (MB/s) = seconds
            print(f"[Node {self.node_id}] Simulating download transfer...")
            time.sleep(transfer_time)
            self.store_replicated_file(file)
            print(f"[Node {self.node_id}] Download successful. Time taken: {transfer_time:.2f} seconds")
        except grpc.RpcError as e:
            print(f"[Node {self.node_id}] Download error: {e}")

    def list_files(self):
        try:
            response = self.stub.ListFiles(storage_pb2.ListFilesRequest())
            print(f"[Node {self.node_id}] Available files:")
            if not response.files:
                print("No files available")
            for f in response.files:
                print(f"- {f.file_id}: {f.file_name} ({f.total_size / (1024 ** 2):.2f} MB)")
        except grpc.RpcError as e:
            print(f"[Node {self.node_id}] List error: {e}")

    def shutdown(self):
        print(f"[Node {self.node_id}] Shutting down...")
        self.heartbeat_sender.stop()
        self.heartbeat_sender.join()
        self.channel.close()
        print(f"[Node {self.node_id}] Shutdown complete")