#include <iostream>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "iouring.hpp"

constexpr size_t BlockSize = 4096;

int main(int argc, char** argv)
{
    std::vector<std::string> args(argv, argv + argc);
    if (argc < 2) {
        std::cerr << "Usage: " << args[0] << " FILE..." << std::endl;
        return 1;
    }

    IoURing ring;
    ring.init();

    struct File {
        std::string name;
        int fd;
        size_t size = 0;
        std::vector<iovec> iovecs = {};
    };

    auto getNumBlocks
        = [](size_t size) { return size / BlockSize + (size % BlockSize > 0 ? 1 : 0); };

    std::vector<File> files;
    files.reserve(argc - 1);
    for (int i = 1; i < argc; ++i) {
        File file { args[i], ::open(args[i].c_str(), O_RDONLY) };
        if (file.fd < 0) {
            std::cerr << "Could not open '" << file.name << std::endl;
            return 1;
        }

        struct stat st;
        if (::fstat(file.fd, &st) < 0) {
            std::cerr << "Could not stat file '" << file.name << std::endl;
            return 1;
        }
        if (!S_ISREG(st.st_mode)) {
            std::cerr << "'" << file.name << "' is not a regular file" << std::endl;
            return 1;
        }
        file.size = st.st_size;

        const auto numBlocks = getNumBlocks(file.size);
        file.iovecs.resize(numBlocks);
        for (size_t block = 0; block < numBlocks; ++block) {
            if (::posix_memalign(&file.iovecs[block].iov_base, BlockSize, BlockSize) != 0) {
                std::cerr << "Error in posix_memalign" << std::endl;
                return 1;
            }
            file.iovecs[block].iov_len = file.size - block * BlockSize;
        }

        const auto sqe = ring.prepareReadv(file.fd, file.iovecs.data(), file.iovecs.size());
        sqe->user_data = files.size();
        files.push_back(std::move(file));
    }
    ring.submitSqes();
    if (!ring.waitCqe(files.size())) {
        std::cerr << "Error waiting for CQEs: "
                  << std::make_error_code(static_cast<std::errc>(errno)).message() << std::endl;
        return 1;
    }

    for (const auto& file : files) {
        const auto cqe = ring.peekCqe();
        if (cqe->res < 0) {
            std::cerr << "Error reading file '" << file.name << "': " << cqe->res << std::endl;
            return 1;
        }
        const auto numBlocks = getNumBlocks(file.size);
        for (size_t block = 0; block < numBlocks; ++block) {
            std::cout << std::string_view(
                static_cast<const char*>(file.iovecs[block].iov_base), file.iovecs[block].iov_len);
            std::free(file.iovecs[block].iov_base);
        }
        ring.advanceCq();
        ::close(file.fd);
    }
}
