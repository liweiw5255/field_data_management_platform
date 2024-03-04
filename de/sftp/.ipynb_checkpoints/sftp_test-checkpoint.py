import os
import DominionEnergySFTP as dm_sftp

# Replace these with your own details
sftp_host = 'secureftp.dominionenergy.com'  # Hostname from dominion energy
sftp_port = 22  # Port 
sftp_username = 'DESC_CLEMSON'  # Clemson Username
sftp_private_key_path = "/home/liweiw/TRACE_PV/data_process/new_driver/de/sftp/clemson_privatekey.pem"  # Path to your private key


dm_sftp_obj = dm_sftp.DominionEnergySFTP(sftp_host, sftp_username, sftp_private_key_path)
dm_sftp_obj.connect()

# read file
in_folder_name = "./outbound/"
in_file_list = dm_sftp_obj.read_file_list(in_folder_name)
print("Folder List: ", in_file_list)

'''
# upload file
upload_folder = "upload_folder/"
local_file_list = os.listdir(upload_folder)
if(len(local_file_list)>0):
    for file in local_file_list:
        dm_sftp_obj.upload(file, upload_folder)
'''
# download file
out_folder_name = "./outbound/"
download_folder = "../de_data/"
out_file_list = dm_sftp_obj.read_file_list(out_folder_name)
for file in out_file_list:
    if(file not in os.listdir(download_folder)):
        dm_sftp_obj.download(file, download_folder)
    else:
        print(file, "existed!")
