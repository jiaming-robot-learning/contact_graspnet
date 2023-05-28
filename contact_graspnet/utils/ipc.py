#!/usr/bin/env python3

import socket
import logging
import numpy as np
from io import BytesIO
import struct
import os

FILE_DIR = os.path.dirname(os.path.abspath(__file__))

def pack_ndarray(frames):
    """
    frames: list of numpy arrays, or a single numpy array
    Returns a bytearray containing the header and the frames
        header: I (4 bytes for unsigned int, total size of body)| H (2 bytes for unsigned short, number of frames)
        body: I (size of each frame) x number of frames | frames
    """
    if not isinstance(frames, list):
        frames = [frames]
    
    if len(frames) == 0:
        return struct.pack('IH', 0, 0)
    
    out = bytearray()
    f = BytesIO()
    sizes = []
    for frame in frames:
        np.savez(f, frame=frame)
        
        packet_size = len(f.getvalue()) - sum(sizes)
        sizes.append(packet_size)

    header = struct.pack('IH', sum(sizes),len(sizes)) # total size of body, number of frames
    out += header # 6 bytes
    for size in sizes:
        header = struct.pack('I', size)
        out += header
    f.seek(0)
    out += f.read()

    return out

def recvall(socket, count):
    # buf = b''
    buf = bytearray()
    while count:
        newbuf = socket.recv(count)
        if not newbuf: return None
        buf += newbuf
        count -= len(newbuf)
    return buf

def send_message(socket, message=None):
    """
    message: [np.array, np.array, ...]
    send a empty message for closing the connection
    """
    if message is None:
        socket.sendall(pack_ndarray([]))
        return
    data = pack_ndarray(message)
    socket.sendall(data)
    
def recv_message(socket):
    """
    receive a message that is packed by pack_ndarray
    """
    header = socket.recv(6)
    if len(header) == 0:
        return None
    total_size, num_frames = struct.unpack('IH', header)
    if total_size == 0:
        return None
    sizes = []
    for _ in range(num_frames):
        size = socket.recv(4)
        size = struct.unpack('I', size)[0]
        sizes.append(size)
    
    body_bytes = recvall(socket, total_size)
    frames = []
    for i in range(num_frames):
        data = BytesIO(body_bytes[:sizes[i]])
        body_bytes = body_bytes[sizes[i]:]
        
        frame = np.load(data, allow_pickle=True)['frame']
        frames.append(frame)
    return frames

class CGNClient():
    def __init__(self,socket_path=None) -> None:
        if socket_path is None:
            socket_path = os.path.join(FILE_DIR,'cgn_socket.sock')
        self.socket_path = socket_path
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.connect(self.socket_path)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def close(self):
        send_message(self.socket)
        self.socket.close()
        
    def get_grasps(self, data):
        """
        data: list of np.array H x W x 5: 
            0-2: rgb
            3: depth
            4: segmap
        
        return: list of np.array
        """
        send_message(self.socket, data)
        response = recv_message(self.socket)
        return response

class CGNServer():
    def __init__(self,process_fn,socket_path=None) -> None:
        if socket_path is None:
            socket_path = os.path.join(FILE_DIR,'cgn_socket.sock')
        self.socket_path = socket_path
        self.process_fn = process_fn
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        os.unlink(self.socket_path)
        self.socket.close()

    def start(self):
        # remove the socket file if it already exists
        try:
            os.unlink(self.socket_path)
        except OSError:
            if os.path.exists(self.socket_path):
                raise OSError(f"Cannot remove socket file {self.socket_path}")

        self.socket.bind(self.socket_path)
        while True:
            self.listen()
            
    def listen(self):

        connection = None
        try:
            self.socket.listen(1)        
            print('Server is listening for incoming connections...')
            connection, client_address = self.socket.accept()
            logging.info('Connection from', str(connection).split(", ")[0][-4:])

            while True:
                data = recv_message(connection)
                if data is None:
                    print('0 bytes received, closing connection...')
                    break

                # print('Received data:', data)
                res = self.process_fn(*data)
                # Send a response back to the client
                send_message(connection, list(res))
                
        finally:
            # close the connection
            print('Closing connection...')
            if connection:
                connection.close()
            # remove the socket file
            
            
 



# class CustomSocket(socket.socket):
#     def sendall(self, frames):
#         if not isinstance(frames, np.ndarray) or not isinstance(frames,list):
#             raise TypeError("Input must be numpy array or list of np arrays") # should this just call super intead?

#         out = self.__pack_frames(frames)
#         super().sendall(out)
#         # logging.debug("frame sent")


#     def recvall(self, count):
#         # buf = b''
#         buf = bytearray()
#         while count:
#             newbuf = super().recv(count)
#             if not newbuf: return None
#             buf += newbuf
#             count -= len(newbuf)
#         return buf

#     def recv(self):
#         length = None
#         frameBuffer = bytearray()
        
#         while True:
#             data = super().recv(bufsize)
#             if len(data) == 0:
#                 return None
#             frameBuffer += data

#             if length is None:
#                 if b':' not in frameBuffer:
#                     break
#                 # remove the length bytes from the front of frameBuffer
#                 # leave any remaining bytes in the frameBuffer!
#                 length_str, ignored, frameBuffer = frameBuffer.partition(b':')
#                 length = int(length_str)

#             if len(frameBuffer) == length:
#                 break

#             # if len(frameBuffer) < length:
#             #     break
#             # # split off the full message from the remaining bytes
#             # # leave any remaining bytes in the frameBuffer!
#             # frameBuffer = frameBuffer[length:]
#             # length = None
#             # break
        
#         frame = np.load(BytesIO(frameBuffer), allow_pickle=True)['frame']
#         logging.debug("frame received")
#         return frame

#     def accept(self):
#         fd, addr = super()._accept()
#         sock = CustomSocket(super().family, super().type, super().proto, fileno=fd)
        
#         if socket.getdefaulttimeout() is None and super().gettimeout():
#             sock.setblocking(True)
#         return sock, addr
    

#     @staticmethod
#     def __pack_frame(frame):
#         """
        
#         """
#         f = BytesIO()
#         np.savez(f, frame=frame)
        
#         packet_size = len(f.getvalue())
#         header = '{0}:'.format(packet_size)
#         header = bytes(header.encode())  # prepend length of array

#         out = bytearray()
#         out += header

#         f.seek(0)
#         out += f.read()
#         return out

#     @staticmethod
#     def __pack_frames(frames):
#         """
#         frames: list of numpy arrays, or a single numpy array
#         Returns a bytearray containing the header and the frames
#             header: I (4 bytes for unsigned int, total size of body)| H (2 bytes for unsigned short, number of frames)
#             body: I (size of each frame) x number of frames | frames
#         """
#         if not isinstance(frames, list):
#             frames = [frames]
        
#         out = bytearray()
#         f = BytesIO()
#         sizes = []
#         for frame in frames:
#             np.savez(f, frame=frame)
            
#             packet_size = len(f.getvalue())
#             sizes.append(packet_size)

#         header = struct.pack('IH', sum(sizes),len(sizes)) # total size of body, number of frames
#         out += header # 6 bytes
#         for size in sizes:
#             header = struct.pack('I', size)
#             out += header
#         f.seek(0)
#         out += f.read()

#         return out