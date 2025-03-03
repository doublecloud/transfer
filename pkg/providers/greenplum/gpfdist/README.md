Source (Storage) UML schema:

@startuml
skinparam sequenceMessageAlign center

Participant Activate
Participant GpfdistStorage
Participant PipesReader
Participant GpfdistBin
Participant Pipes
Participant ExternalTable

activate GpfdistStorage

Activate -> GpfdistStorage: LoadTable(pusher)

GpfdistStorage -> GpfdistBin: Init GpfdistBin
activate GpfdistBin

GpfdistBin -> Pipes: []syscall.MkFifo(file)
activate Pipes

GpfdistStorage -> PipesReader: Create PipesReader
activate PipesReader
GpfdistStorage --> PipesReader: Run(pusher)

PipesReader -> GpfdistBin: Open pipes as read only

GpfdistBin -> Pipes: []os.OpenFile(pipe)
GpfdistBin <- Pipes: []os.File
PipesReader <- GpfdistBin: []os.File

PipesReader -> Pipes: Read pipes

GpfdistStorage -> GpfdistBin: Start read through external table

GpfdistBin -> ExternalTable: Create writable external table and start insert
activate ExternalTable

Pipes <-- ExternalTable: TSV Data
PipesReader <-- Pipes: TSV Data
PipesReader --> PipesReader: pusher(TSV Data)

ExternalTable -> GpfdistBin: Exported rows count
deactivate ExternalTable
GpfdistBin -> GpfdistStorage: Exported rows count

GpfdistStorage -> PipesReader: Wait for result

PipesReader -> Pipes: Close pipes

GpfdistStorage <-- PipesReader: Pushed rows count

deactivate PipesReader

GpfdistStorage -> GpfdistBin: Stop

GpfdistBin -> Pipes: []os.Remove(pipe)
deactivate GpfdistBin

GpfdistStorage -> Activate: Result
deactivate GpfdistStorage
