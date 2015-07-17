#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <vector>
#include <unistd.h>
#include <errno.h>
#include <iostream>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <dirent.h>
#include <stdint.h>

/* Global mutex used to ensure there is no line mixed in output */
static boost::mutex staticLoggerMutex;
class LoggerMutex {
public:
    LoggerMutex() : _lock(staticLoggerMutex) {}
    ~LoggerMutex() {}

private:
    boost::mutex::scoped_lock _lock;

private:
    LoggerMutex(const LoggerMutex&);
    LoggerMutex& operator=(const LoggerMutex&);
};

class MMapedFile
{
public:
    MMapedFile(const char* filename) : fd(-1), mmapData(NULL), mmapDataLen(0), initialized(false) {
        fd = open(filename, O_RDONLY);
        if (fd == -1) {
            return;
        }
        struct stat s;
        if (fstat(fd, &s) == -1) {
            const char* msg = strerror(errno);
            LoggerMutex lock;
            std::cerr << " unable to get stats for " << filename << ":" << msg << std::endl;
            close(fd);
            fd = -1;
            return;
        }
        if (!S_ISREG(s.st_mode)) {
            close(fd);
            fd = -1;
            return;
        }
        // Handle the empty file case
        if (s.st_size == 0) {
            close(fd);
            fd = -1;
            mmapDataLen = 0;
            mmapData = NULL;
            initialized = true;
            return;
        }

        mmapData = mmap (0, s.st_size, PROT_READ, MAP_SHARED | MAP_NORESERVE, fd, 0);
        if (mmapData == MAP_FAILED) {
            const char* msg = strerror(errno);
            LoggerMutex lock;
            std::cerr << "Unable to mmap " << filename << ":" << msg << std::endl;
            mmapData = NULL;
            close(fd);
            return;
        }
        madvise(mmapData, s.st_size, MADV_RANDOM);
        mmapDataLen = s.st_size;
        initialized = true;
    }

    ~MMapedFile() 
    {
        if (mmapData != NULL)
            munmap(mmapData, mmapDataLen);
        if (fd != -1)
            close(fd);
    }

    bool loaded() const
    { 
        return initialized; 
    }

    size_t len() const
    { 
        return mmapDataLen;
    }

    const char* content() const
    {
        return (const char*)mmapData; 
    }

private:
    int       fd;
    void*     mmapData;
    size_t    mmapDataLen;
    bool      initialized;
};

/**
 * Enumerate content of a directory
 */
class DirContentEnumerator
{
public:
    DirContentEnumerator(const char* dir) : _dp(NULL), _dir(NULL) {
        _dir = opendir(dir);
        unsigned len = offsetof(struct dirent, d_name) + pathconf(dir, _PC_NAME_MAX) + 1;
        _dp = (struct dirent*)malloc(len);
        next();
    }
    ~DirContentEnumerator() {
        free(_dp);
        if (_dir != NULL) {
            closedir(_dir);
        }
    }

public:
    bool end() {
      return _dir == NULL || _path.size() == 0;
    }
    void next() {
        _path.resize(0);
        _isDir = false;
        if (_dir == NULL) {
            return;
        }
        struct dirent* res = NULL;
        if (readdir_r(_dir, _dp, &res) != 0 || res == NULL) {
            return;
        }
        if (_dp->d_name[0] == '.' && (_dp->d_name[1] == 0 || 
                                     (_dp->d_name[1] == '.' || _dp->d_name[2] == 0))) {
            return next();
        }
        _isDir = (_dp->d_type & DT_DIR);
        _path.insert(_path.end(), _dp->d_name, _dp->d_name + strlen(_dp->d_name) + 1);
    }

    // return true if current entry is a directory
    bool isDir() const {
        return _isDir;
    }
    // return the name of the current entry
    const char* get() const {
        return &_path[0];
    }
    
private:
    struct dirent*      _dp;
    DIR*                _dir;
    std::vector<char>   _path;
    bool                _isDir;

private:
    DirContentEnumerator();
    DirContentEnumerator(const DirContentEnumerator&);
    DirContentEnumerator& operator=(const DirContentEnumerator&);
};

class DetectCorruption
{
public:
    DetectCorruption(const std::string &dataDir) : _dataDir(dataDir) {
        boost::thread t(boost::bind(DetectCorruption::_mainLoop, this));
    }

private:
    static void _mainLoop(DetectCorruption *self) {
        std::string path, path2, path3, file;
        path.append(self->_dataDir);
        path2.reserve(1024);
        path3.reserve(1024);
        file.reserve(1024);

        while (true) {
            DirContentEnumerator dirEnum(path.c_str());

            // Scan All Folder
            while (!dirEnum.end()) {
                if (!dirEnum.isDir()) {
                    self->_checkFile(path, dirEnum.get(), file);
                } else {
                    path2.resize(0);
                    path2.append(path);
                    path2.push_back('/');
                    path2.append(dirEnum.get());

                    DirContentEnumerator dirEnum2(path2.c_str());
                    // Scan all files
                    while (!dirEnum2.end()) {
                        int len = strlen(dirEnum2.get());
                        if (len > 4 && memcmp(dirEnum2.get() + len - 4, ".tmp", 4) == 0) {
                        // ignore .tmp files
                        } else {
                            self->_checkFile(path2, dirEnum2.get(), file);
                        }
                        dirEnum2.next();
                    }
                }
                dirEnum.next();
            }
            sleep(1);
        }
    }

    void _checkFile(const std::string &path, const char *file, std::string &filename) {
        filename.resize(0);
        filename.append(path);
        filename.push_back('/');
        filename.append(file);
        MMapedFile mmap(filename.c_str());
        if (mmap.loaded()) {
            bool corrupted = false;
            // Detect all 512-bytes page inside the file filled by 0 -> can be caused by a buggy Trim
            for (unsigned i = 0; !corrupted && i < mmap.len(); i += 512) {
                if (mmap.len() - i > 4) { // only check page > 4-bytes to avoid false positive
                    bool pagecorrupted = true; 
                    for (unsigned j = i; j < mmap.len() && j < (i + 512); ++j) {                    
                        if (mmap.content()[j] != 0)
                            pagecorrupted = false;
                    }
                    if (pagecorrupted)
                        corrupted = true;

                }
            }
            if (corrupted) {
                std::cerr << "Corrupted file found: " << filename << std::endl;
                exit(1);
            }

        }
    }

private:
    std::string                      _dataDir;
};

bool sync(const char* path)
{
  int fd = open(path, O_RDONLY);
  if (fd == -1)
    return false;
  if (fsync(fd) != 0) {
    close(fd);
    return false;
  }
  return (close(fd) == 0);
}

void writeAtomically(const std::string &dataDir, unsigned folder, unsigned file, uint64_t size, unsigned seed)
{
    std::stringstream ss;
    ss << dataDir << "/" << folder;
    std::string folderPath = ss.str();
    mkdir(dataDir.c_str(), 0777);
    mkdir(folderPath.c_str(), 0777);
    ss << "/" << file;
    std::string destFile = ss.str();
    ss << ".tmp";
    std::string tmpFile = ss.str();
    unsigned char buff[65535];

    for (unsigned i = 0; i < 65536; ++i) {
        buff[i] = (seed + i) % 256;
    }
    { // write file1.bin.tmp
        FILE *file = fopen(tmpFile.c_str(), "wb");
        assert(file != NULL);
        uint64_t sizeToWrite = size;

        while (sizeToWrite > 0) {
            uint64_t toWrite = (sizeToWrite > 65536 ? 65536 : sizeToWrite);
            uint64_t nb = fwrite(buff, 1, toWrite, file);
            if (nb != toWrite) {
                std::cerr << "Disk full, rm folder & restart the test" << std::endl;
                exit(0);
            }
            sizeToWrite -= toWrite;
        }
        fflush(file);
        fsync(fileno(file));
        fclose(file);
    }
    // be sure meta data of filesystem are sync
    sync(folderPath.c_str());
    rename(tmpFile.c_str(), destFile.c_str());
    sync(folderPath.c_str());
}

void deleteFile(const std::string &dataDir, unsigned folder, unsigned file)
{
    std::stringstream ss;
    ss << dataDir << "/" << folder << "/" << file;
    unlink(ss.str().c_str());
}

class WriteThread
{
public:
    WriteThread(const std::string &dataDir, unsigned instance, unsigned nbLoop) {
        _dataDir = dataDir;
        _instance = instance;
        _nbLoop = nbLoop;
        _t = boost::thread(boost::bind(WriteThread::_mainLoop, this));
    }
    void join() {
        _t.join();
    }
private:
    static void _mainLoop(WriteThread *self) {
        uint64_t size = 100 * 1024 * 1024; // 100MB
        unsigned loop = 0;

        // write 2 small files
        writeAtomically(self->_dataDir, self->_instance, 10 * self->_nbLoop + 1, 972, self->_instance);
        writeAtomically(self->_dataDir, self->_instance, 10 * self->_nbLoop + 2, 148, self->_instance);

        while (loop < self->_nbLoop) {
            // write the file
            writeAtomically(self->_dataDir, self->_instance, loop + 1, size, self->_instance);
            writeAtomically(self->_dataDir, self->_instance, 10 * self->_nbLoop + 3, 528, self->_instance);
            writeAtomically(self->_dataDir, self->_instance, 10 * self->_nbLoop + 4, 2789, self->_instance);
            deleteFile(self->_dataDir, self->_instance, self->_nbLoop);
            size += 100 * 1024 * 1024; // 100MB
            ++loop;
        }
    }

private:
    std::string    _dataDir;
    unsigned       _instance;
    unsigned       _nbLoop;
    boost::thread  _t;
};

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << argv[0] << " path" << std::endl;
        std::cout << std::endl;
        std::cout << "path: path to write the data " << std::endl;
        return 1;
    }
    std::string dataDir(argv[1]);
    DetectCorruption detectCorruption(dataDir);
    std::vector<WriteThread*> threads;
    
    // write 1024 small files
   for (unsigned i = 0; i < 1024; ++i) {
      writeAtomically(dataDir, i, 1024 /* file 1024 */, 8 + i /* size */, i/* seed for content */);
    }
    for (unsigned i = 0; i < 8; ++i) {
        threads.push_back(new WriteThread(dataDir, i, 10 + 100 * i));
    }
    for (unsigned i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
    return 0;
}
