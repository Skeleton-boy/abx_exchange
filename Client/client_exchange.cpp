#include <iostream>
#include <vector>
#include <map>
#include <set>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdint>
#include <algorithm>

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #pragma comment(lib, "ws2_32.lib")
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    #define closesocket close
#endif

class JSONSerializer {
public:
    static std::string serialize(const std::vector<std::map<std::string, std::string>>& records) {
        std::ostringstream oss;
        oss << "[\n";
        for (size_t i = 0; i < records.size(); ++i) {
            oss << "  {\n";
            for (auto it = records[i].begin(); it != records[i].end(); ++it) {
                oss << "    \"" << it->first << "\": \"" << it->second << "\"";
                if (std::next(it) != records[i].end()) oss << ",";
                oss << "\n";
            }
            oss << "  }";
            if (i < records.size() - 1) oss << ",";
            oss << "\n";
        }
        oss << "]";
        return oss.str();
    }
};

class PacketProcessor {
public:
    static std::map<std::string, std::string> decodePacket(const unsigned char* data) {
        std::map<std::string, std::string> packet;
        packet["symbol"] = std::string(reinterpret_cast<const char*>(data), 4);
        packet["buysellindicator"] = std::string(1, data[4]);
        packet["quantity"] = std::to_string(convertBytesToUint32(data + 5));
        packet["price"] = std::to_string(convertBytesToUint32(data + 9));
        packet["packetSequence"] = std::to_string(convertBytesToUint32(data + 13));
        return packet;
    }

private:
    static uint32_t convertBytesToUint32(const unsigned char* bytes) {
        return (static_cast<uint32_t>(bytes[0]) << 24) |
               (static_cast<uint32_t>(bytes[1]) << 16) |
               (static_cast<uint32_t>(bytes[2]) << 8) |
               static_cast<uint32_t>(bytes[3]);
    }
};

class SocketManager {
public:
    SocketManager() : socket_fd(-1) {
#ifdef _WIN32
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
            std::cerr << "WSAStartup failed\n";
            exit(1);
        }
#endif
    }

    bool establishConnection(const char* ip, int port) {
        socket_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (socket_fd == -1) {
            std::cerr << "Socket creation error\n";
            return false;
        }

        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(port);

#ifdef _WIN32
        server_address.sin_addr.s_addr = inet_addr(ip);
#else
        inet_pton(AF_INET, ip, &server_address.sin_addr);
#endif

        if (::connect(socket_fd, (struct sockaddr*)&server_address, sizeof(server_address)) == -1) {
            std::cerr << "Connection failed\n";
            return false;
        }

        return true;
    }

    void sendRequest(uint8_t requestType, uint8_t sequenceNumber = 0) {
        unsigned char request[2] = {requestType, sequenceNumber};
        send(socket_fd, reinterpret_cast<const char*>(request), 2, 0);
    }

    int receiveData(unsigned char* buffer, int bufferSize) {
        return recv(socket_fd, reinterpret_cast<char*>(buffer), bufferSize, 0);
    }

    void setReceiveTimeout(int seconds) {
#ifdef _WIN32
        DWORD timeout = seconds * 1000;
        setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
#else
        struct timeval timeout;
        timeout.tv_sec = seconds;
        timeout.tv_usec = 0;
        setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
#endif
    }

    ~SocketManager() {
        if (socket_fd != -1) {
            closesocket(socket_fd);
        }
#ifdef _WIN32
        WSACleanup();
#endif
    }

private:
    int socket_fd;
};

class ExchangeClient {
public:
    ExchangeClient() : server_ip("127.0.0.1"), server_port(3000) {}

    bool connect() {
        return socketManager.establishConnection(server_ip, server_port);
    }

    void retrieveData() {
        socketManager.sendRequest(1);
        unsigned char buffer[17];
        socketManager.setReceiveTimeout(3);

        while (true) {
            int bytesRead = socketManager.receiveData(buffer, 17);
            if (bytesRead <= 0) {
                std::cout << "No more data. Stopping reception.\n";
                break;
            }

            auto packet = PacketProcessor::decodePacket(buffer);
            int sequence = std::stoi(packet["packetSequence"]);
            packets[sequence] = packet;
            std::cout << "Received packet: " << sequence << " - " << packet["symbol"] << std::endl;
        }

        identifyMissingSequences();
        requestMissingPackets();
        saveDataAsJSON("output.json");
    }

private:
    const char* server_ip;
    int server_port;
    SocketManager socketManager;
    std::map<int, std::map<std::string, std::string>> packets;
    std::set<int> missingSequences;

    void identifyMissingSequences() {
        int maxSequence = 0;
        for (const auto& entry : packets) {
            maxSequence = std::max(maxSequence, std::stoi(entry.second.at("packetSequence")));
        }

        for (int i = 1; i <= maxSequence; ++i) {
            if (packets.find(i) == packets.end()) {
                missingSequences.insert(i);
            }
        }
    }

    void requestMissingPackets() {
        unsigned char buffer[17];
        for (int seq : missingSequences) {
            socketManager.sendRequest(2, seq);
            int bytesRead = socketManager.receiveData(buffer, 17);
            if (bytesRead > 0) {
                auto packet = PacketProcessor::decodePacket(buffer);
                packets[std::stoi(packet["packetSequence"])] = packet;
                std::cout << "Received missing packet: " << packet["packetSequence"] << " - " << packet["symbol"] << std::endl;
            }
        }
    }

    void saveDataAsJSON(const std::string& filename) {
        std::vector<std::map<std::string, std::string>> data;
        for (const auto& entry : packets) {
            data.push_back(entry.second);
        }

        std::ofstream file(filename);
        file << JSONSerializer::serialize(data);
        file.close();

        std::cout << "JSON file saved: " << filename << std::endl;
    }
};

int main() {
    ExchangeClient client;
    if (!client.connect()) {
        std::cerr << "Failed to connect to server" << std::endl;
        return 1;
    }

    client.retrieveData();
    return 0;
}