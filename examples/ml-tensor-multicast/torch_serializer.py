"""
Tensor serializer based on torch.save() (inspired by Monarch implementation).
"""
import io
from typing import Any
import torch


def serialize_tensor(tensor: torch.Tensor) -> bytes:
    """
    Serialize a tensor to bytes using torch.save.
        
    Example:
        >>> tensor = torch.randn(10, 20)
        >>> data = serialize_tensor(tensor)
        >>> node.send_packet(data)
    """
    buf = io.BytesIO()
    # uses torch.save, disable new zipfile format for better performance.
    torch.save(tensor, buf, _use_new_zipfile_serialization=False)
    return buf.getvalue()


def deserialize_tensor(data: bytes, device: str = "cpu") -> torch.Tensor:
    """
    Deserialize bytes to a tensor using torch.load.
        
    Example:
        >>> data = node.recv_packet()
        >>> tensor = deserialize_tensor(data)
    """
    buf = io.BytesIO(data)
    tensor = torch.load(buf, map_location=device, weights_only=False)
    return tensor


def serialize_object(obj: Any) -> bytes:
    """
    Serialize any Python object (including tensors).
    
    Args:
        obj: Any serializable object
        
    Returns:
        Serialized bytes
    """
    buf = io.BytesIO()
    torch.save(obj, buf, _use_new_zipfile_serialization=False)
    return buf.getvalue()


def deserialize_object(data: bytes, device: str = "cpu") -> Any:
    """
    Deserialize any Python object.
    
    Args:
        data: Serialized bytes
        device: Target device for tensors
        
    Returns:
        Deserialized object
    """
    buf = io.BytesIO(data)
    obj = torch.load(buf, map_location=device, weights_only=False)
    return obj


class TorchSerializer:
    """
    Serializer based on torch.save/load (class interface).
    
    Example:
        >>> serializer = TorchSerializer()
        >>> data = serializer.serialize(tensor)
        >>> restored = serializer.deserialize(data)
    """
    
    def __init__(self, device: str = "cpu"):
        """
        Args:
            device: Target device for deserialization
        """
        self.device = device
    
    def serialize(self, obj: Any) -> bytes:
        """Serialize object."""
        return serialize_object(obj)
    
    def deserialize(self, data: bytes) -> Any:
        """Deserialize object."""
        return deserialize_object(data, device=self.device)


to_bytes = serialize_tensor
from_bytes = deserialize_tensor

