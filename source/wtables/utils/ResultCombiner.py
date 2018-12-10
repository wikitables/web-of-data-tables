class ResultCombiner:
    def __init__(self, fileout):
        self.outputFile=fileout

    def init(self):
        self.file = open(self.outputFile, "w")

    def __call__(self, result):
        if result!=None:
            self.file.write(result)

    def shutdown(self):
        # close the file
        self.file.close()
