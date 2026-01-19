# conda install paramiko
# pip3 install --upgrade --force-reinstall cryptography

import paramiko
import sys


class DominionEnergySFTP:
    
    def __init__(self, host, username, auth_path):
        self.host = host
        self.username = username
        self.port = 22
        self.auth_path = auth_path
        self.sftp = None
       
    def close(self):
        if(self.sftp is None):
            print("SFTP Session not exists!")
        else:    
            # Close the SFTP and SSH sessions if they were opened
            if 'sftp' in locals() and sftp:
                self.sftp.close()
            if 'ssh' in locals() and ssh:
                self.ssh.close()
        
    def connect(self):
        try:
            # Create an SSH client instance
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Load your private key
            private_key = paramiko.RSAKey.from_private_key_file(self.auth_path)
    
            # Connect to the server
            ssh.connect(self.host, port=self.port, username=self.username, pkey=private_key)

            # Open an SFTP session
            self.sftp = ssh.open_sftp()
            print("Connect to SFTP Server Succeed!")

        except FileNotFoundError:
            print("Error: The private key file was not found.", file=sys.stderr)
        except paramiko.ssh_exception.NoValidConnectionsError:
            print("Error: Could not connect to the SFTP server. Check the host and port.", file=sys.stderr)
        except paramiko.ssh_exception.AuthenticationException:
            print("Error: Authentication failed. Check your username and private key.", file=sys.stderr)
        except paramiko.SSHException as e:
            print(f"SSH error occurred: {e}", file=sys.stderr)
        except Exception as e:
            print(f"An unexpected error occurred: {e}", file=sys.stderr)
        

    def read_file_list(self, folder_name):
        if(self.sftp is None):
            print("SFTP Session not exists!")
        else:
            # Perform SFTP operations (example: list directory contents)
            return self.sftp.listdir(folder_name)
                # read file content
                #with self.sftp.file("./inbound/"+filename, 'r') as file:
                #    file_content = file.read()
                #    print("\nFile content:")
                #    print(file_content.decode('utf-8'))  

    
    
    def upload(self, upload_filename, upload_path):  
        local_filename = upload_path + upload_filename
        remote_filename = "./inbound/" + upload_filename
        # check if the file exists already
        inbound_file_list = self.sftp.listdir('./inbound/')
   
        if(upload_filename in inbound_file_list):
            print(upload_filename + " exists!")
            return 0
        
        try:
            self.sftp.put(local_filename, remote_filename) 
            print(upload_filename + " upload succeed!")
            return 1
        except Exception as e:
            print("Upload Error: " + str(e))
            return -1

            
    def download(self, download_filename, download_path):
        local_filename = download_path + download_filename
        remote_filename = "./outbound/" + download_filename
        
        print(local_filename)
        print(remote_filename)
      
        try:
            self.sftp.get(remote_filename, local_filename)
            print(download_filename + " download succeed!")
        except Exception as e:
            print("Download Error: " + str(e))
  