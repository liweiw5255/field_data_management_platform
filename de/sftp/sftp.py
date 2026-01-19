# conda install paramiko

import paramiko
import os
import sys
sys.path.append('../')

print(sys.path)
#from path import path



# Replace these with your own details
sftp_host = 'secureftp.dominionenergy.com'  # Hostname from dominion energy
sftp_port = 22  # Port 
sftp_username = 'DESC_CLEMSON'  # Clemson Username
private_key_path = "de/sftp/clemson_privatekey.pem"  # Path to your private key

print(type(private_key_path))
try:
    # Create an SSH client instance
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Load your private key
    private_key = paramiko.RSAKey.from_private_key_file(private_key_path)

    # Connect to the server
    ssh.connect(sftp_host, port=sftp_port, username=sftp_username, pkey=private_key)

    # Open an SFTP session
    sftp = ssh.open_sftp()

    # Perform SFTP operations (example: list directory contents)
    print("Directory contents:")
    for filename in sftp.listdir('.'):
        print(filename)

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
finally:
    # Close the SFTP and SSH sessions if they were opened
    if 'sftp' in locals() and sftp:
        sftp.close()
    if 'ssh' in locals() and ssh:
        ssh.close()
