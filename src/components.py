from abc import ABC,abstractmethod

import polars as pl
import gcsfs



class DataExtractor:
    """Class that downloads data and stores it for use in pipeline steps"""
    def __init__(self,season) -> None:
        self.season = season

    def extract(self):
        if self.season >= 2016:
            self.pbp = pl.read_parquet(f'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{self.season}.parquet')
            self.pbp_part = pl.read_parquet(f'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{self.season}.parquet')
            self.stats = pl.read_parquet(f'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_{self.season}.parquet')
            self.players = pl.read_parquet('https://github.com/nflverse/nflverse-data/releases/download/players/players.parquet')
        else:
            self.pbp = pl.read_parquet(f'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{self.season}.parquet')
            self.pbp_part = None
            self.stats = pl.read_parquet(f'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_{self.season}.parquet')
            self.players = pl.read_parquet('https://github.com/nflverse/nflverse-data/releases/download/players/players.parquet')



class BasePipeline(ABC):

    @abstractmethod
    def extract(self,dataload: DataExtractor):
        pass

    @abstractmethod
    def transform(self):
        pass

    def load(self):
        fs = gcsfs.GCSFileSystem(project='handy-reference-305822',token='creds.json')
        with fs.open(f'rawnfldata/nflverse/{self.table}_{self.season}.parquet','wb') as f:
            self.data.write_parquet(f)

class DimGame(BasePipeline):
    def __init__(self) -> None:
        self.table = 'games'

    def extract(self,dataload: DataExtractor):
        self.season = dataload.season
        self.raw_data = dataload.pbp
        return self

    def transform(self):
        self.data = self.raw_data.select([
                    pl.col('game_id'),
                    pl.col('old_game_id'),
                    pl.col('home_team'),
                    pl.col('away_team'),
                    pl.col('season_type'),
                    pl.col('season'),
                    pl.col('week'),
                    pl.col('game_date').str.to_date(),
                    pl.col('start_time').str.to_datetime(format = '%m/%d/%y, %H:%M:%S',strict=False),
                    pl.col('stadium'),
                    pl.col('weather'),
                    pl.col('nfl_api_id'),
                    pl.col('home_score'),
                    pl.col('away_score'),
                    pl.col('location'),
                    pl.col('result'),
                    pl.col('total'),
                    pl.col('spread_line'),
                    pl.col('total_line'),
                    pl.col('div_game'),
                    pl.col('roof'),
                    pl.col('surface'),
                    pl.col('temp'),
                    pl.col('wind'),
                    pl.col('home_coach'),
                    pl.col('away_coach'),
                    pl.col('stadium_id'),
                    pl.col('game_stadium'),
                ]).unique()
        #print(self.data)
        return self
        

class Pbp(BasePipeline):
    def __init__(self) -> None:
        self.table = 'pbp'
        


    def extract(self,dataload: DataExtractor):
        self.season = dataload.season
        self.raw_data = dataload.pbp
        self.raw_part = dataload.pbp_part
        return self
    
    def transform(self):
        exlucde_cols = ['game_id',
                        'old_game_id',
                        'home_team',
                        'away_team',
                        'season_type',
                        'season',
                        'week',
                        'game_date',
                        'start_time',
                        'stadium',
                        'weather',
                        'nfl_api_id',
                        'home_score',
                        'away_score',
                        'location',
                        'result',
                        'total',
                        'spread_line',
                        'total_line',
                        'div_game',
                        'roof',
                        'surface',
                        'temp',
                        'wind',
                        'home_coach',
                        'away_coach',
                        'stadium_id',
                        'game_stadium',
                        'time_of_day']
        
        pbp = self.raw_data.select([
                    pl.col('game_id'),
                    pl.col('time_of_day'),#.str.to_datetime(),
                    pl.exclude(exlucde_cols)
                ])
        if self.season >= 2016:
            part = self.raw_part.select([
                            pl.col('nflverse_game_id'),
                            pl.col('play_id').cast(pl.Float64),
                            pl.col('offense_formation'),
                            pl.col('offense_personnel'),
                            pl.col('defenders_in_box'),
                            pl.col('defense_personnel'),
                            pl.col('number_of_pass_rushers'),
                            pl.col('n_offense'),
                            pl.col('n_defense')
                    ])
            self.data = pbp.join(
                part,
                left_on = ['game_id','play_id'],
                right_on = ['nflverse_game_id','play_id'],
                how = 'left'
            )
        else:
            self.data = pbp
        
        return self
    

        

class PbpPart(BasePipeline):
    def __init__(self) -> None:
        self.table = 'pbppart'

    def extract(self,dataload: DataExtractor):
        self.season = dataload.season
        self.raw_data =  dataload.pbp_part
        return self

    def transform(self):
        if self.season < 2016:
            return self
        self.data = self.raw_data.filter(
                        pl.col('players_on_play') != ''
                    ).select([
                        pl.col('nflverse_game_id').alias('game_id'),
                        pl.col('play_id'),
                        pl.concat_str([
                            pl.col('offense_players'),
                            pl.col('defense_players')
                        ],separator = ';').str.split(';').alias('player_id')
                    ]).explode('player_id')
        
        return self
    

    def load(self):
        if self.season < 2016:
            return self
        else:
            super().load()
    
class Players(BasePipeline):
    def __init__(self) -> None:
        self.table = 'players'

    def extract(self,dataload: DataExtractor):
        self.season = dataload.season
        self.raw_data = dataload.players
        return self

    def transform(self):
        self.data = self.raw_data
        return self

    def load(self):
        fs = gcsfs.GCSFileSystem(project='handy-reference-305822',token='creds.json')
        with fs.open(f'rawnfldata/nflverse/{self.table}.parquet','wb') as f:
            self.data.write_parquet(f)


class Stats(BasePipeline):
    def __init__(self) -> None:
        self.table = 'stats'


    def extract(self,dataload: DataExtractor):
        self.season = dataload.season
        self.raw_data = dataload.stats
        self.pbp = dataload.pbp

        return self

    def transform(self):
        games = self.pbp.select([
                    pl.col('game_id'),
                    pl.col('old_game_id'),
                    pl.col('home_team'),
                    pl.col('away_team'),
                    pl.col('season_type'),
                    pl.col('season'),
                    pl.col('week'),
                    pl.col('game_date').str.to_date(),
                    pl.col('start_time'),#.str.to_datetime(format = '%m/%d/%Y')
                    pl.col('stadium'),
                    pl.col('weather'),
                    pl.col('nfl_api_id'),
                    pl.col('home_score'),
                    pl.col('away_score'),
                    pl.col('location'),
                    pl.col('result'),
                    pl.col('total'),
                    pl.col('spread_line'),
                    pl.col('total_line'),
                    pl.col('div_game'),
                    pl.col('roof'),
                    pl.col('surface'),
                    pl.col('temp'),
                    pl.col('wind'),
                    pl.col('home_coach'),
                    pl.col('away_coach'),
                    pl.col('stadium_id'),
                    pl.col('game_stadium'),
                ]).unique()
        self.data = self.raw_data.join(
                    games,
                    left_on = ['recent_team','season','week'],
                    right_on = ['home_team','season','week'],
                    how = 'left'
                ).join(
                    games,
                    left_on = ['recent_team','season','week'],
                    right_on = ['away_team','season','week'],
                    how = 'left',
                    suffix = 'right2'
                ).select([
                    pl.coalesce([pl.col('game_id'),pl.col('game_idright2')]),
                    pl.col(self.raw_data.columns)
                ]).select([
                    pl.exclude(['season','week','season_type'])
                ])
        
        return self