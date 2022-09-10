from threading import Lock

class AsyncCsvWriterByMap:

    expectedValues = {}
    expectedKeys = []
    values = {}
    fileHandler = None

    def __init__(self, expectedValues, expectedKeys, fileName):
        self.expectedKeys = expectedKeys
        self.expectedValues = expectedValues

        self.fileHandler = open(fileName, 'a')
        self.fileHandler.truncate(0)

        self.fileHandler.write("Index;Total;"+";".join(expectedKeys)+"\n")

        self.lock = Lock()
            
    def flush(self):
        self.lock.acquire()

        for i in self.expectedValues.copy():
            totalCount = 0

            if i in self.values:
                for j in self.values[i]:
                    totalCount = totalCount + self.values[i][j]

                if totalCount == self.expectedValues[i]:

                    values = [str(i), str(totalCount)]

                    for j in self.expectedKeys:

                        if j in self.values[i]:
                            values.append(str(self.values[i][j]))
                        else:
                            values.append("0")

                    self.fileHandler.write(";".join(values) + "\n")
                    self.fileHandler.flush()

                    del self.expectedValues[i]
                else:
                    break
            else: break

        if len(self.expectedValues.keys()) == 0:
            self.fileHandler.close()

        self.lock.release()


    def incrementType(self, index, type):

        if not index in self.values:
            self.values[index] = {}

        if not type in self.values[index]:
            self.values[index][type] = 0

        self.values[index][type] = self.values[index][type] + 1

        self.flush()