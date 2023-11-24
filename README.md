# NFLVerse Pipeline

This repo contains a data pipeline that gets data from the NFLverse project, transforms the schema and uploads it google cloud storage. By storing the data in google cloud storage it can be efficiently queried using Duckdb.

# Pipeline Components

The pipeline is made of a general pipeline class which orchestrates the step classes that perform the ETL for the individual tables.


```{python}
class Pipeline:
    def __init__(self,season: int, steps: list[BasePipeline]) -> None:
        self.season = season
        self.steps = steps
        

    def run(self):
        data = DataExtractor(self.season)
        data.extract()
        for step in self.steps:
            step.extract(data).transform().load()
```

The code above shows that the Pipeline init function takes two arguments: the season we wish to extract and a list of steps. Which correspond to the tables we wish to load into cloud storage. After initializing the pipeline we call its run method which runs the pipeline. It first creates a DataExtractor object and calls its extract method. Where it loads all the data needed for the pipeline steps. Because some files from NFLVerse are used in multiple steps this bundles all the downloads into one object to be used across steps. We then loop over all the steps and call their extract, transform, and load methods. This runs the ETL for these tables.