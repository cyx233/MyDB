#ifndef FILE_SYSTEM_MANAGER_H
#define FILE_SYSTEM_MANAGER_H
#include "bufmanager/BufPageManager.h"
#include "fileio/FileManager.h"
class FileSystemManager
{
public:
    static FileSystemManager *get_instance()
    {
        if (file_sys_manager_)
            return file_sys_manager_;
        return file_sys_manager_ = new FileSystemManager();
    };
    static BufPageManager *get_buf_manager()
    {
        return get_instance()->buf_manager_;
    };
    static FileManager *get_file_manager()
    {
        return get_instance()->file_manager_;
    }

private:
    FileSystemManager()
    {
        file_manager_ = new FileManager();
        buf_manager_ = new BufPageManager(file_manager_);
    };
    ~FileSystemManager()
    {
        delete buf_manager_;
        delete file_manager_;
        delete file_sys_manager_;
        buf_manager_ = nullptr;
        file_manager_ = nullptr;
        file_sys_manager_ = nullptr;
    };
    BufPageManager *buf_manager_ = nullptr;
    FileManager *file_manager_ = nullptr;
    static FileSystemManager *file_sys_manager_;
};

FileSystemManager *FileSystemManager::file_sys_manager_ = nullptr;

#endif