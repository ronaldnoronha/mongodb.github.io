
class Query:
    def __init__(self, db, col, pipeline):
        self.db = db
        self.col = col
        self.pipeline = pipeline

    def getResults(self):
        return self.db[self.col].aggregate(self.pipeline)

    def getExecStats(self):
        return self.db.command('explain', {'aggregate':self.col, 'pipeline':self.pipeline, 'cursor':{}},
                               verbosity='executionStats')

    def getQueryCostVector(self):
        execStats = self.getExecStats()
        dct = {}
        for i in list(execStats['shards']):
            if 'stages' in list(execStats['shards'][i]):
                dct[i] = execStats['shards'][i]['stages'][0]['$cursor']['executionStats']['totalDocsExamined']
            else:
                dct[i] = execStats['shards'][i]['executionStats']['totalDocsExamined']
        total = 0
        for i in dct:
            total += dct[i]
        return total

    def getQueryExecTime(self):
        execStats = self.getExecStats()
        dct = {}
        for i in list(execStats['shards']):
            if 'stages' in list(execStats['shards'][i]):
                dct[i] = execStats['shards'][i]['stages'][1]['executionTimeMillisEstimate']
            else:
                dct[i] = execStats['shards'][i]['executionStats']['executionTimeMillis']
        max = 0
        for i in dct:
            if dct[i] > max:
                max = dct[i]
        return max
        total = 0
        for i in dct:
            total += dct[i]
        return total
