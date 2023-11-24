from components import DataExtractor,BasePipeline,DimGame,Pbp,PbpPart,Players,Stats


class Pipeline:
    def __init__(self,season: int, steps: list[BasePipeline]) -> None:
        self.season = season
        self.steps = steps
        

    def run(self):
        data = DataExtractor(self.season)
        data.extract()
        for step in self.steps:
            step.extract(data).transform().load()



if __name__ == '__main__':
    steps = [DimGame(),Pbp(),PbpPart(),Players(),Stats()]
    #for season in range(2016,2023):
    #    pipe = Pipeline(season = season,steps = steps)
    #    pipe.run()
    pipe = Pipeline(season = 2023,steps = steps)
    pipe.run()