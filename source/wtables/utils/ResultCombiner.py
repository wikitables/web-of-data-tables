import gzip
class ResultCombiner:
    def __init__(self, fileName):
        self.fileName=fileName
        """print(nameFiles)
        self.listNameFiles=[]
        self.listFiles=[]
        for i in range(len(nameFiles)):
            self.listNameFiles.append(nameFiles[i])
            self.listFiles.append(None)"""

    def init(self):
        self.fileName=open(self.fileName, "w")
        #print(self.listNameFiles)
        #for i in range(len(self.listNameFiles)):
        #    self.listFiles[i]=open(self.listNameFiles[i],"w")

    def __call__(self, result):
        if result!=None and result!="":
            self.fileName.write(result)
            #res=result.split("RESULT")
            #if len(res)!=len(self.listFiles):
            #    self.shutdown()
            #    raise Exception("{} lines given for {} files".format(len(res), len(self.listFiles)))

            #for i, line in enumerate(res):
            #    self.listFiles[i].write(line)

    def shutdown(self):
        # close the file
        self.fileName.close()
        #for i in range(len(self.listFiles)):
        #    self.listFiles[i].close()
