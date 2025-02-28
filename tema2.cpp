#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <algorithm>
#include <queue>

using namespace std;

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 64
#define MAX_CHUNKS 100

#define MAX_BUFFER 1000

#define SEEDS_REQUEST 6
#define SEEDS_RESPONSE_FILENAME_NUMCHUNKS 7
#define SEEDS_RESPONSE_CHUNKS 8
#define SEEDS_RESPONSE_SEEDS 9
#define CHUNK_REQUEST 10
#define CHUNK_RESPONSE 11
#define COMPLETED_FILE 12
#define COMPLETED_ALL_FILES 13
#define FINISH 14

// Structura pentru informatiile despre un fisier
struct FileData {
    string fileName;
    int numChunks;
    vector<string> chunks;
};

// Structura pentru informatiile despre un client
struct ClientData {
    vector<FileData> ownedFiles;
    vector<string> desiredFiles;
};

// Structura pentru informatiile despre un fisier in swarm
struct FileSwarm {
    string fileName;
    int numChunks;
    vector<string> chunks;
    vector<int> seeds;
};

// Structura pentru mesaje
struct Message {
    int type;
    int sourceRank;
    char fileName[MAX_FILENAME];
    char chunk[HASH_SIZE];
    int chunkIndex;
    int seed;
};

// Structura pentru prioritatea seed-urilor
struct SeedPriority {
    int seedRank;
    int usageCount;

    bool operator>(const SeedPriority& other) const {
        return usageCount > other.usageCount;
    }
};

// Vector pentru datele clientilor
vector<ClientData> clientData;
// Vector pentru datele despre fisierele din swarm
vector<FileSwarm> fileSwarm;
// Coada de prioritate pentru seed-uri
using SeedPriorityQueue = priority_queue<SeedPriority, vector<SeedPriority>, greater<SeedPriority>>;

// Returneaza indexul fisierului in swarm
int findFileSwarm(const string &fileName)
{
    for (size_t i = 0; i < fileSwarm.size(); i++)
    {
        if (fileSwarm[i].fileName == fileName)
        {
            return i;
        }
    }
    return -1;
}

// Parsarea fisierelor de input
void parseInputFile(int rank)
{
    string input_file = "in" + to_string(rank) + ".txt";
    ifstream fin(input_file);
    if (!fin.is_open())
    {
        cerr << "Error opening file: " << input_file << endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    vector<FileData> ownedFiles;
    int numOwnedFiles;
    fin >> numOwnedFiles;

    for (int i = 0; i < numOwnedFiles; i++)
    {
        FileData file;
        fin >> file.fileName >> file.numChunks;

        file.chunks.resize(file.numChunks);
        for (int j = 0; j < file.numChunks; j++)
        {
            fin >> file.chunks[j];
        }
        ownedFiles.push_back(file);
    }

    vector<string> desiredFiles;
    int numDesiredFiles;
    fin >> numDesiredFiles;
    for (int i = 0; i < numDesiredFiles; i++)
    {
        string file;
        fin >> file;
        desiredFiles.push_back(file);
    }

    clientData[rank].ownedFiles = ownedFiles;
    clientData[rank].desiredFiles = desiredFiles;

    fin.close();
}

// Trimiterea datelor clientului catre tracker
void sentClientDataToTracker(int rank)
{
    int numOwnedFiles = clientData[rank].ownedFiles.size();
    // Numarul de fisiere detinute
    MPI_Send(&numOwnedFiles, 1, MPI_INT, TRACKER_RANK, 1, MPI_COMM_WORLD);
    for (const auto &file : clientData[rank].ownedFiles)
    {
        // Numele fisierului
        MPI_Send(file.fileName.c_str(), file.fileName.size() + 1, MPI_CHAR, TRACKER_RANK, 2, MPI_COMM_WORLD);
        // Numarul de chunk-uri
        MPI_Send(&file.numChunks, 1, MPI_INT, TRACKER_RANK, 3, MPI_COMM_WORLD);
        // Chunk-urile
        for (const auto &chunk : file.chunks)
        {
            MPI_Send(chunk.c_str(), chunk.size() + 1, MPI_CHAR, TRACKER_RANK, 4, MPI_COMM_WORLD);
        }
    }

    // Confirmare ca s-au trimis toate fisierele
    char ack[10];
    MPI_Recv(ack, 10, MPI_CHAR, TRACKER_RANK, 5, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

// Primeste datele de la clienti
void receiveClientDataFromClients(int numtasks)
{
    for (int i = 1; i < numtasks; i++)
    {
        int numOwnedFiles;
        // Primeste numarul de fisiere detinute
        MPI_Recv(&numOwnedFiles, 1, MPI_INT, i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        for (int j = 0; j < numOwnedFiles; j++)
        {
            char file_name[MAX_FILENAME];
            // Primeste numele fisierului
            MPI_Recv(file_name, MAX_FILENAME, MPI_CHAR, i, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            string fileName(file_name);
            int numChunks;
            // Primeste numarul de chunk-uri
            MPI_Recv(&numChunks, 1, MPI_INT, i, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            vector<string> chunks(numChunks);
            for (int k = 0; k < numChunks; k++)
            {
                vector<char> chunk(HASH_SIZE);
                // Primeste chunk-urile
                MPI_Recv(chunk.data(), HASH_SIZE, MPI_CHAR, i, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                chunks[k] = string(chunk.begin(), chunk.end());
            }

            // Adauga fisierul in swarm
            int index = findFileSwarm(fileName);
            if (index == -1)
            {
                FileSwarm newFileSwarm{fileName, numChunks, chunks, {i}};
                fileSwarm.push_back(newFileSwarm);
            }
            else
            {
                auto &seeds = fileSwarm[index].seeds;
                int newSeed = i;
                if (find(seeds.begin(), seeds.end(), newSeed) == seeds.end())
                {
                    seeds.insert(seeds.begin(), newSeed);
                }
            }
        }

        // Confirmare ca s-au primit toate fisierele
        string ack = "ACK";
        MPI_Send(ack.c_str(), ack.size() + 1, MPI_CHAR, i, 5, MPI_COMM_WORLD);
    }
}

// Updateaza swarm-ul cu informatii noi
void updateFileSwarm(int rank)
{
    for (const auto& downloadFile : clientData[rank].desiredFiles)
    {
        // Trimite cerere pentru informatii despre fisier
        Message request;
        request.type = SEEDS_REQUEST;
        request.sourceRank = rank;
        strcpy(request.fileName, downloadFile.c_str());
        MPI_Send(&request, sizeof(Message), MPI_CHAR, TRACKER_RANK, SEEDS_REQUEST, MPI_COMM_WORLD);

        // Primeste informatii despre fisier
        MPI_Status status;
        Message response;
        MPI_Recv(&response, sizeof(Message), MPI_CHAR, TRACKER_RANK, SEEDS_RESPONSE_FILENAME_NUMCHUNKS, MPI_COMM_WORLD, &status);
        int numChunks = response.chunkIndex;

        vector<string> chunks;
        for (int i = 0; i < numChunks; i++)
        {
            Message responseChunk;
            MPI_Recv(&responseChunk, sizeof(Message), MPI_CHAR, TRACKER_RANK, SEEDS_RESPONSE_CHUNKS, MPI_COMM_WORLD, &status);
            chunks.push_back(responseChunk.chunk);
        }

        vector<int> seeds;
        Message responseSeed;
        MPI_Recv(&responseSeed, sizeof(Message), MPI_CHAR, TRACKER_RANK, SEEDS_RESPONSE_SEEDS, MPI_COMM_WORLD, &status);
        seeds.push_back(responseSeed.seed);
        int numSeeds = responseSeed.chunkIndex;

        for (int i = 1; i < numSeeds; i++)
        {
            Message responseSeed;
            MPI_Recv(&responseSeed, sizeof(Message), MPI_CHAR, TRACKER_RANK, SEEDS_RESPONSE_SEEDS, MPI_COMM_WORLD, &status);
            seeds.push_back(responseSeed.seed);
        }

        // Adauga fisierul in swarm
        int index = findFileSwarm(downloadFile);
        if (index == -1)
        {
            FileSwarm newFileSwarm;
            newFileSwarm.fileName = downloadFile;
            newFileSwarm.numChunks = numChunks;
            newFileSwarm.chunks = chunks;
            newFileSwarm.seeds = seeds;
            fileSwarm.push_back(newFileSwarm);
        } else {
            for (const auto& seed : seeds)
            {
                if (find(fileSwarm[index].seeds.begin(), fileSwarm[index].seeds.end(), seed) == fileSwarm[index].seeds.end())
                {
                    fileSwarm[index].seeds.push_back(seed);
                }
            }
        }
    }
}

// Functia executata de thread-ul de download
void *download_thread_func(void *arg)
{
    int rank = *(int *)arg;

    // Parcurgem fisierele dorite de client
    for (const auto& downloadFile : clientData[rank].desiredFiles)
    {
        int chunksDownloaded = 0;
        // Trimite cerere pentru informatii despre fisier
        Message request;
        request.type = SEEDS_REQUEST;
        request.sourceRank = rank;
        strcpy(request.fileName, downloadFile.c_str());
        MPI_Send(&request, sizeof(Message), MPI_CHAR, TRACKER_RANK, SEEDS_REQUEST, MPI_COMM_WORLD);

        // Primeste informatii despre fisier
        MPI_Status status;
        Message response;
        MPI_Recv(&response, sizeof(Message), MPI_CHAR, TRACKER_RANK, SEEDS_RESPONSE_FILENAME_NUMCHUNKS, MPI_COMM_WORLD, &status);
        int numChunks = response.chunkIndex;

        vector<string> chunks;
        for (int i = 0; i < numChunks; i++)
        {
            Message responseChunk;
            MPI_Recv(&responseChunk, sizeof(Message), MPI_CHAR, TRACKER_RANK, SEEDS_RESPONSE_CHUNKS, MPI_COMM_WORLD, &status);
            chunks.push_back(responseChunk.chunk);
        }

        vector<int> seeds;
        Message responseSeed;
        MPI_Recv(&responseSeed, sizeof(Message), MPI_CHAR, TRACKER_RANK, SEEDS_RESPONSE_SEEDS, MPI_COMM_WORLD, &status);
        seeds.push_back(responseSeed.seed);
        int numSeeds = responseSeed.chunkIndex;

        for (int i = 1; i < numSeeds; i++)
        {
            Message responseSeed;
            MPI_Recv(&responseSeed, sizeof(Message), MPI_CHAR, TRACKER_RANK, SEEDS_RESPONSE_SEEDS, MPI_COMM_WORLD, &status);
            seeds.push_back(responseSeed.seed);
        }

        // Adauga fisierul in swarm
        FileSwarm newFileSwarm;
        newFileSwarm.fileName = downloadFile;
        newFileSwarm.numChunks = numChunks;
        newFileSwarm.chunks = chunks;
        newFileSwarm.seeds = seeds;
        fileSwarm.push_back(newFileSwarm);

        int index = findFileSwarm(downloadFile);

        // Initializare coada de prioritate pentru seed-uri
        SeedPriorityQueue seedQueue;
        for (int seed : seeds)
        {
            seedQueue.push({seed, 0});
        }

        // Descarca chunk-urile de la cele mai disponibile seed-uri
        while (!seedQueue.empty()) {
            SeedPriority selectedSeed = seedQueue.top();
            seedQueue.pop();

            int seedRank = selectedSeed.seedRank;
            for (const auto& chunk : chunks)
            {
                // Trimite cerere pentru chunk
                Message requestChunk;
                requestChunk.type = CHUNK_REQUEST;
                requestChunk.sourceRank = rank;
                strcpy(requestChunk.fileName, downloadFile.c_str());
                strcpy(requestChunk.chunk, chunk.c_str());
                requestChunk.chunkIndex = distance(fileSwarm[index].chunks.begin(), find(fileSwarm[index].chunks.begin(), fileSwarm[index].chunks.end(), chunk));

                int retry = 0;
                bool chunkReceived = false;

                // Primeste mesajul cu chunk-ul si retrimite cererea daca nu a primit chunk-ul
                while (retry < 1 && !chunkReceived)
                {
                    MPI_Send(&requestChunk, sizeof(Message), MPI_CHAR, seedRank, CHUNK_REQUEST, MPI_COMM_WORLD);

                    MPI_Status status;
                    Message responseChunk;
                    MPI_Recv(&responseChunk, sizeof(Message), MPI_CHAR, seedRank, CHUNK_RESPONSE, MPI_COMM_WORLD, &status);
                    
                    if (responseChunk.chunkIndex == -1)
                    {
                        retry++;
                    }
                    else
                    {
                        chunkReceived = true;
                        chunksDownloaded++;
                    }
                }
                
                // Actualizeaza seed-ul in coada
                selectedSeed.usageCount++;
                seedQueue.push(selectedSeed);

                // Updateaza swarm-ul la fiecare 10 chunk-uri descarcate
                if (chunksDownloaded % 10 == 0)
                {
                    updateFileSwarm(rank);
                }
            }

            break;
        }

        // Trimite mesajul cu faptul ca a terminat de descarcat fisierul
        Message completedFile;
        completedFile.type = COMPLETED_FILE;
        completedFile.sourceRank = rank;
        strcpy(completedFile.fileName, downloadFile.c_str());
        MPI_Send(&completedFile, sizeof(Message), MPI_CHAR, TRACKER_RANK, COMPLETED_FILE, MPI_COMM_WORLD);

        // Adauga fisierul in lista de fisiere detinute
        FileData newFile;
        newFile.fileName = downloadFile;
        newFile.numChunks = numChunks;
        newFile.chunks = chunks;
        clientData[rank].ownedFiles.push_back(newFile);

        // Scrie in fisierul de output
        string outputFileName = "client" + to_string(rank) + "_" + downloadFile;
        ofstream outputFile(outputFileName, ios::out);

        if (outputFile.is_open())
        {
            for (const auto &chunk : fileSwarm[index].chunks)
            {
                outputFile << chunk << endl;
            }
            outputFile.close();
        }
    }

    // Trimite mesajul cu faptul ca a terminat de descarcat toate fisierele dorite
    Message completedAllFiles;
    completedAllFiles.type = COMPLETED_ALL_FILES;
    completedAllFiles.sourceRank = rank;
    MPI_Send(&completedAllFiles, sizeof(Message), MPI_CHAR, TRACKER_RANK, COMPLETED_ALL_FILES, MPI_COMM_WORLD);

    return NULL;
}

// Functia executata de thread-ul de upload
void *upload_thread_func(void *arg)
{
    int rank = *(int*)arg;

    while (true)
    {
        // Primeste un mesaj de la un client
        MPI_Status status;
        Message request;
        MPI_Recv(&request, sizeof(Message), MPI_CHAR, MPI_ANY_SOURCE, CHUNK_REQUEST, MPI_COMM_WORLD, &status);

        // Daca mesajul este de tip CHUNK_REQUEST, cauta si trimite chunk-ul cerut
        if (request.type == CHUNK_REQUEST)
        {
            string fileName = request.fileName;
            string chunk = request.chunk;
            int chunkIndex = request.chunkIndex;
            int index = -1;

            // Cauta fisierul in lista de fisiere detinute
            for (int i = 0; i < clientData[rank].ownedFiles.size(); i++)
            {
                if (clientData[rank].ownedFiles[i].fileName == fileName)
                {
                    index = i;
                    break;
                }
            }

            if (index != -1)
            {
                Message response;
                response.type = CHUNK_RESPONSE;

                // Daca chunk-ul cerut se afla in lista de chunk-uri detinute, trimite-l
                if (clientData[rank].ownedFiles[index].chunks[chunkIndex] == chunk)
                {
                    response.sourceRank = rank;
                    strcpy(response.fileName, fileName.c_str());
                    strcpy(response.chunk, chunk.c_str());
                    response.chunkIndex = chunkIndex;
                    MPI_Send(&response, sizeof(Message), MPI_CHAR, request.sourceRank, CHUNK_RESPONSE, MPI_COMM_WORLD);
                }
                // Altfel, trimite un mesaj de reincercare
                else 
                {
                    response.sourceRank = rank;
                    strcpy(response.fileName, fileName.c_str());
                    strcpy(response.chunk, chunk.c_str());
                    response.chunkIndex = -1;
                    MPI_Send(&response, sizeof(Message), MPI_CHAR, request.sourceRank, CHUNK_RESPONSE, MPI_COMM_WORLD);
                }
            } else {
                continue;
            }
        }
        // Daca mesajul este de tip FINISH, iese din bucla
        else if (request.type == FINISH)
        {
            break;
        }
    }

    return NULL;
}

// Functia executata de tracker
void tracker(int numtasks, int rank)
{
    receiveClientDataFromClients(numtasks);
    int completedClients = 0;

    while (true)
    {
        // Primeste un mesaj de la un client
        MPI_Status status;
        Message receivedMessage;
        MPI_Recv(&receivedMessage, sizeof(Message), MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        int sourceRank = receivedMessage.sourceRank;

        switch (receivedMessage.type)
        {
            // Daca mesajul este de tip SEEDS_REQUEST, trimite informatii despre fisier
            case SEEDS_REQUEST:
            {
                string fileName = receivedMessage.fileName;
                int index = findFileSwarm(fileName);

                Message response;
                response.type = SEEDS_RESPONSE_FILENAME_NUMCHUNKS;
                response.sourceRank = TRACKER_RANK;
                strcpy(response.fileName, fileName.c_str());
                response.chunkIndex = fileSwarm[index].numChunks;
                MPI_Send(&response, sizeof(Message), MPI_CHAR, sourceRank, SEEDS_RESPONSE_FILENAME_NUMCHUNKS, MPI_COMM_WORLD);

                for (int i = 0; i < fileSwarm[index].numChunks; i++)
                {
                    Message responseChunk;
                    responseChunk.type = SEEDS_RESPONSE_CHUNKS;
                    responseChunk.sourceRank = TRACKER_RANK;
                    strcpy(responseChunk.fileName, fileName.c_str());
                    strcpy(responseChunk.chunk, fileSwarm[index].chunks[i].c_str());
                    responseChunk.chunkIndex = i;
                    MPI_Send(&responseChunk, sizeof(Message), MPI_CHAR, sourceRank, SEEDS_RESPONSE_CHUNKS, MPI_COMM_WORLD);
                }

                for (const auto& seed : fileSwarm[index].seeds)
                {
                    Message responseSeed;
                    responseSeed.type = SEEDS_RESPONSE_SEEDS;
                    responseSeed.sourceRank = TRACKER_RANK;
                    responseSeed.chunkIndex = fileSwarm[index].seeds.size();
                    strcpy(responseSeed.fileName, fileName.c_str());
                    responseSeed.seed = seed;
                    MPI_Send(&responseSeed, sizeof(Message), MPI_CHAR, sourceRank, SEEDS_RESPONSE_SEEDS, MPI_COMM_WORLD);
                }
                break;
            }

            // Daca mesajul este de tip COMPLETED_FILE, adauga seed-ul in swarm
            case COMPLETED_FILE:
            {
                string fileName = receivedMessage.fileName;
                int index = findFileSwarm(fileName);
                fileSwarm[index].seeds.push_back(receivedMessage.sourceRank);
                break;
            }

            // Daca mesajul este de tip COMPLETED_ALL_FILES, trimite mesaj de terminare
            case COMPLETED_ALL_FILES:
            {
                completedClients++;

                if (completedClients == numtasks - 1)
                {
                    for (int i = 1; i < numtasks; i++)
                    {
                        Message completedAllFiles;
                        completedAllFiles.type = FINISH;
                        completedAllFiles.sourceRank = TRACKER_RANK;
                        MPI_Send(&completedAllFiles, sizeof(Message), MPI_CHAR, i, CHUNK_REQUEST, MPI_COMM_WORLD);
                    }

                    return;
                }
            }
            default:
                break;
        }
    }
}

// Functia executata de peer
void peer(int numtasks, int rank)
{
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    parseInputFile(rank);
    sentClientDataToTracker(rank);

    r = pthread_create(&download_thread, NULL, download_thread_func, (void*)&rank);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void*)&rank);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }
}

int main(int argc, char *argv[])
{
    int numtasks, rank;

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE)
    {
        cerr << "Error: MPI does not support multi-threading!" << endl;
        exit(-1);
    }

    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK)
    {
        clientData.resize(numtasks);
        tracker(numtasks, rank);
    }
    else
    {
        clientData.resize(numtasks);
        peer(numtasks, rank);
    }

    MPI_Finalize();
    return 0;
}
