import socket
import requests
import struct

def send_to_aveva(data, bearer,):
    """sends dictionary to AVEVA Insight as JSON for a Data Source"""
    
    headers = headers = {
                    "Content-Type": "application/json",
                    "Authorization": bearer
                         }
    data = {"data": [data]}
    return  requests.post("https://online.wonderware.com/apis/upload/datasource",
                        json = data,
                        headers = headers)

def hex_bytes(num):
    data = hex(num)[2:]
    prepend = 4 - len(data)
    for i in range(0,prepend):
        data = '0'+data
    return f" {data[:2]} {data[2:]} "

def int_to_bool_array(n):
	bits = bin(n)[2:].zfill(8)
	output = []
	for bit in bits:
		if int(bit):
			output.append(True)
		else:
			output.append(False)
	output.reverse()
	return output

class ModBusTcp:

    def __init__(self, ip, port=502, msg_header="01 02 00 00 00 06 01"):
        self.ip = ip
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM,0)
        self.header = msg_header
        self.port = port


    def __enter__(self):
        self.sock.connect( (self.ip, self.port) )

    def __exit__(self, *exc_details):
        self.sock.close()
    
    def connect(self):
        self.sock.connect( (self.ip, self.port) )    

    def close(self):
        self.sock.close()
              
        
    def __call__(self,address, quantity = 1, func_type = 3):
        if func_type == 3:
            return self.read_register(address, quantity)
        elif func_type == 1:
            return self.read_coil(address, quantity)
        raise ValueError("Invalid Function Type")
            

        
    def read_coil(self, coil, quantity=1):
        
        data = bytes.fromhex(self.header+" 01 "+hex_bytes(coil) +
                             hex_bytes(quantity))
        self.sock.send(data)
        receive = self.sock.recv(1024)
        num_bytes = receive[8]

        coil_status = struct.unpack(str(num_bytes) + 'b', receive[9:])
        output = []
        for num in coil_status:
            output += int_to_bool_array(num)
        return output
        
    

    def read_register(self, register, quantity=1):
        data = bytes.fromhex(self.header+" 03 "+hex_bytes(register) +
                             hex_bytes(quantity))
        self.sock.send(data)
        receive = self.sock.recv(1024)
        num_bytes = receive[8]

        return struct.unpack('>' + str(num_bytes//2) + 'h', receive[9:])
        
    
    def __str__(self):
        return f"PLC with IP = {self.ip}"
    

   


