
class AsyncCsvWriterBySequentialMap:

    map = {}
    fileHandler = None

    def __init__(self, total, header, fileName):

        self.map = dict.fromkeys((range(total)))

        self.fileHandler = open(fileName, 'a')
        self.fileHandler.truncate(0)

        self.fileHandler.write(";".join(header)+"\n")

    def flush(self):
        
        for i in self.map.copy():
            if not self.map[i] == None:
                self.fileHandler.write(";".join(self.map[i]) + "\n")
                self.fileHandler.flush()
                del self.map[i]
            else: break

    def setValue(self, index, value):
        if not index in self.map:
            raise Exception("Index does not exist, range: 0-{}".format(len(self.map)))

        self.map[index] = value
        self.flush()