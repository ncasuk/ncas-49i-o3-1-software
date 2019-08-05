import pysftp
import os
import stat
import pathlib
import inspect
import multiprocessing

def init_process(sftpServerName,sftpU,sftpKey,sftpP,cnopts):
    global sftp
    sftp = pysftp.Connection(sftpServerName, username=sftpU, private_key=sftpKey, private_key_pass=sftpP,cnopts=cnopts)

def recursive_copy(sftp,remote_dir,local_dir,excluded_files = None,file_extensions = None):
    copy_files,copy_dirs = list_remote(sftp,remote_dir,excluded_files,file_extensions)
    local_files,local_dirs = list_local(local_dir,file_extensions)
    copy_files = shorten_flist(copy_files,remote_dir)
    local_files = shorten_flist(local_files,local_dir)
    copy_dirs = shorten_dirs(copy_dirs,remote_dir)
    local_dirs = shorten_dirs(local_dirs,local_dir)
    final_files = list(set(copy_files) - set(local_files))
    final_dirs = list(set(copy_dirs) - set(local_dirs))
    create_local_dirs(local_dir,final_dirs)
    print(len(final_files),"files to copy")
    copy_files_over(sftp,final_files[0:10],local_dir,remote_dir)
    return final_files,final_dirs

def copy_files_over(sftp,final_files,local_dir,remote_dir):
    for i in final_files:
        copy_file(sftp,i[0],local_dir,remote_dir)
        
def copy_file(sftp,fname,local_dir,remote_dir):
    print(fname)
    sftp.get(str(pathlib.Path(remote_dir + fname).as_posix()),localpath = str(pathlib.Path(local_dir + fname)),callback = None, preserve_mtime = True)

def create_local_dirs(local_dir,dlist):
    for i in dlist:
        os.makedirs(str(pathlib.Path(local_dir + i)),exist_ok = True)
        
def shorten_flist(flist,to_remove):
    flist = [(pathlib.Path(i[0][len(to_remove):]).as_posix(),i[1]) for i in flist]
    return flist
def shorten_dirs(dlist,to_remove):
    dlist = [pathlib.Path(i[len(to_remove):]).as_posix() for i in dlist]
    return dlist

def list_remote(sftp,remote_dir,excluded_files,file_extensions):
    files = []
    dirs = []
    c_dir_listing = sftp.listdir_attr(remote_dir)
    for i in c_dir_listing:
        if (stat.S_ISDIR(i.st_mode)):
            dirs.append(remote_dir + "/" + i.filename)
            temp_files,temp_dirs = list_remote(sftp,remote_dir + "/" + i.filename,excluded_files,file_extensions)
            files += temp_files
            dirs += temp_dirs
        elif (stat.S_ISREG(i.st_mode)):
            if file_extensions != None:
                if any(i.filename.lower().endswith(ext) for ext in file_extensions):
                    files.append((remote_dir + "/" + i.filename,i.st_mtime))
            else:
                files.append((remote_dir + "/" + i.filename,i.st_mtime))
        else:
            print(i.st_mode,remote_dir + "/" + i.filename,"not synced")
    return files,dirs
    
def list_local(path,file_extensions):
    file_list = []
    dir_list = []
    for root, dirs, files in os.walk(path):
        for currentFile in files:
            if file_extensions != None:
                if any(currentFile.lower().endswith(ext) for ext in file_extensions):
                    file_list.append((os.path.join(root, currentFile),os.path.getmtime(os.path.join(root, currentFile))))
            else:
                file_list.append((os.path.join(root, currentFile),os.path.getmtime(os.path.join(root, currentFile))))
        dir_list += dirs
    return file_list,dir_list

if __name__ == "__main__":
    curr_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    conf_dir = os.path.join(curr_dir,"config")
    parent_dir = str(pathlib.Path(curr_dir).parent)
    sftpKey = os.path.join(conf_dir,'private_key_openSSH')
    sftpServerName = 'jasmin-xfer1.ceda.ac.uk'
    sftpU = 'martynwarduk'
    sftpP = 'Jumbos1979'
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    remote_dir = "/gws/nopw/j04/ncas_obs/amf/raw_data/ncas-gc-ms-tof-1/data"
    local_dir = os.path.join(parent_dir,"Input_data")
    sftp = pysftp.Connection(sftpServerName, username=sftpU, private_key=sftpKey, private_key_pass=sftpP,cnopts=cnopts)
    files_to_copy,dirs_to_create = recursive_copy(sftp,remote_dir,local_dir)
    sftp.close()
