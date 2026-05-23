# -*- coding: utf-8 -*-
"""gRPC 服务端（预留，当前主要使用 REST API）"""

def create_server(port: int = 50051):
    """创建 gRPC 服务器（需安装 grpcio 和 grpcio-tools）"""
    try:
        import grpc
        server = grpc.server(__import__('concurrent.futures', fromlist=['ThreadPoolExecutor']).ThreadPoolExecutor(max_workers=4))
        server.add_insecure_port(f"[::]:{port}")
        return server
    except ImportError:
        raise RuntimeError("需要安装 grpcio: pip install grpcio grpcio-tools")
