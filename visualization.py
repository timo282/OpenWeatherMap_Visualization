from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame
import plotly.express as px
from Kafka import KafkaReader
import uuid
import numpy as np
from utils import icon_dict, map_func, map_func_speed, wind_strength_dict, direction_dict, cmap, wind_legend_text1, icon_legend_text1, wind_legend_text2, icon_legend_text2, add_recommendation
from IPython.display import clear_output


class StaticWeatherMapVisualizer():
    """
    Can be used to visualize different types of static weather maps.
    Superclass of DynamicWeatherMapVisualizer.
    """
    def __init__(self):
        
        self.list_of_figures = [] # this list will contain (fig, type)-tuples for each figure created with this class. 
                                  # fig is the figure-object/instance, type is the kind of weather map (temp, icon or wind)
        self.df = pd.DataFrame({}) # this dataframe will contain the data fetched from the cleaned kafka topic (these data will be visualized in the maps)
    
    def get_dataframe(self, kafka_con:KafkaReader) -> DataFrame:
        """
        Fetches data from kafka-topic "weather.forecast.clean" and converts it into a pandas dataframe. 
        Additionally, some columns are added and old data are dropped.
        Stores the dataframe in the instance variable self.df.
        """
        data = kafka_con.retrieve()  # consumer fetches data from kafka topic
        df = pd.json_normalize(data) # data is normalized and written into a dataframe

        # weather-elements have to be normalized manually
        weather_data = df["weather"].apply(lambda x: x[0])
        df_weather = pd.json_normalize(weather_data)
        df = pd.concat((df, df_weather), axis=1)
        df = df.drop("weather", axis=1)

        df["temp_rounded"] = df["main.temp"].apply(lambda x: int(np.round(x, 0))).astype(str) + "°C" # add rounded temperature string column

        df["Date"] = df["dt_txt"].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')) # restructure date

        df["icon_symbol"] = df["icon"].apply(lambda x: icon_dict.get(x, "")) # add column with icon symbol using icon_dict from utils

        # add column with wind direction symbol (arrow) using direction_dict and map_func from utils
        df["wind_symbol"] = df["wind.deg"].apply(lambda x: direction_dict.get(map_func(x), ""))
        # add column wind speed symbol using wind_strength_dict and map_func_speed from utils
        df["wind_strength"] = df["wind.speed"].apply(lambda x: wind_strength_dict.get(map_func_speed(x), ""))
        df["wind"] = df["wind_symbol"] + df["wind_strength"] # concatenate wind direction and wind strength symbols
        
        df["recommendation"] = df.apply(lambda x: add_recommendation(x), axis=1) # add recommendations using add_recommendation from utils

        # filter old forecast
        df_clean = pd.DataFrame().reindex(columns=df.columns)
        df["date_time_city"] = df["dt_txt"] + df["city.name"]
        for x in df["date_time_city"].unique(): # if two forecasts are for the same city and the same time, use only the most current one
            tmp = df.loc[df["date_time_city"]==x]
            latest_row = tmp.sort_values("fetched_at", ascending=False).iloc[0]
            df_clean.loc[len(df_clean)] = latest_row

        # fix dtypes
        df_clean["main.temp"] = df_clean["main.temp"].astype(float)
        df_clean["dt"] = df_clean["dt"].astype(float)

        # filter data to current date --> drop all forecasts for time before now
        timestamp_now = datetime.timestamp(datetime.now()) # note that this can be shifted compared to the time in your timezone,
        df_clean = df_clean.drop(df_clean[df_clean["dt"]<timestamp_now].index)    # this may the reason if you still see e.g. the forecast for 9:00 at 10:00

        self.df = df_clean # store data in instance variable self.df


    def create_temperature_map(self) -> None:
        """
        Creates and shows a temperature weather map and appends it to the instance variable self.list_of_figures.
        """
        fig = self._create_map( # use _create_map and specify the parameters to create a temperature map figure
            hover_data=["city.name", "main.temp", "main.pressure", "main.humidity", "description", "main.feels_like", "wind.speed", "wind.deg"], 
            text="temp_rounded", 
            title="5-day Temperature Forecast Germany", 
            hovertemplate=('<b>%{customdata[0]}</b><br>'+'<br><i>%{customdata[4]}</i>'+
                           '<br><b>Temperature</b>: %{customdata[1]}°C (feels like: %{customdata[5]}°C)'+
                           '<br><b>Pressure</b>: %{customdata[2]} hPa'+'<br><b>Humidity</b>: %{customdata[3]}%'+
                           '<br><b>Wind</b>: speed: %{customdata[6]} m/s, direction: %{customdata[7]}°'), 
            slider_prefix="Date=",
            scatter_size=20,
            color="main.temp",
            text_size=10
        )
        
        # customize colorbar
        fig.update_coloraxes(cmax=37, cmin=5, colorbar={"title": 'Temperature in °C'}) # min and max may be adjusted in winter to see differences better

        self.list_of_figures.append((fig, "temp")) # append figure and type to list_of_figures
        fig.show() # show figure in notebook cell output
        print('\n')
        
        
    def create_icon_map(self) -> None:
        """
        Creates and shows a weather map with icons and recommendations and appends it to the instance variable self.list_of_figures.
        """
        fig = self._create_map( # use _create_map and specify the parameters to create an icon map figure
            hover_data=["city.name", "main.temp", "description", "recommendation"], 
            text="icon_symbol", 
            title="5-day Weather Forecast Germany<br><i>(hover over icons to see recommended actions)</i>", 
            hovertemplate=('<b>%{customdata[0]}</b><br>'+ '<br><i>%{customdata[2]}</i>'+ '<br><b>Temperature</b>: %{customdata[1]}°C' + 
                           '<br><b>Recommendations</b>:<i>%{customdata[3]}</i>'), 
            slider_prefix=f"{icon_legend_text1}<br>{icon_legend_text2}<br><br> Date=",
            scatter_size=0
        )
                
        self.list_of_figures.append((fig, "icon"))
        fig.show()
        print('\n')
        
        
    def create_wind_map(self) -> None:
        """
        Creates and shows a wind map and appends it to the instance variable self.list_of_figures.
        """
        fig = self._create_map( # use _create_map and specify the parameters to create a wind map figure
            hover_data=["city.name", "wind.speed", "wind.deg"], 
            text="wind", 
            title="5-day Wind Forecast Germany",
            hovertemplate='<b>%{customdata[0]}</b><br>'+'<br><b>Wind Speed</b>: %{customdata[1]} m/s'+'<br><b>Wind Direction</b>: %{customdata[2]}°', 
            slider_prefix=f"{wind_legend_text1}<br>{wind_legend_text2} <br><br> Date=",
            scatter_size=0
        )
        
        self.list_of_figures.append((fig, "wind"))   
        fig.show()
        print('\n')
        
        
    def _create_map(self, hover_data:list, text:str, title:str, hovertemplate:str, slider_prefix:str, scatter_size:int, text_size=12, color:str=None):
        """
        Creates a basis map figure, which can be customized to a special type of map (see above) by specifying the methods' parameters.
        """
        if self.df.empty: # fetch data only if they have not been fetched yet (to avoid long, evitable loading times for maps)
            if self.__class__.__name__ == "StaticWeatherMapVisualizer":  # different usage of get_dataframe method depending on which type of class is used (static/dynamic; dynamic class see below)
                self.get_dataframe(KafkaReader(bootstrap_servers='kafka-1,kafka-2',  # initialize KafkaReader and fetch data to dataframe with get_dataframe() method
                                               topic='weather.forecast.clean', 
                                               group_id=uuid.uuid4().hex, 
                                               client_id=uuid.uuid4().hex, 
                                               auto_offset_reset='earliest'
                                              )
                                  )
            elif self.__class__.__name__ == "DynamicWeatherMapVisualizer":
                self.get_dataframe()
        
        forecasts = self.df

        fig = px.scatter_geo(  # create scatter_geo figure with data from dataframe and values specified in the methods' parameters
             forecasts, 
             lat="city.coords.latitude", # some values are the same for all types of map, e.g. lat and lon
             lon="city.coords.longitude",
             color=color,
             color_continuous_scale=cmap,
             scope="europe", 
             animation_frame="Date", 
             animation_group="city.name", 
             hover_name="city.name",
             hover_data=hover_data,
             width=800, height=600,
             text=text,
             size=[scatter_size]*len(forecasts),
             title=title
        )
        
        fig.update_layout(  # adjust layout of figure
            geo = dict(
                projection_scale=7.5,             # zoom in
                center=dict(lat=51.32, lon=9.50), # to center of Germany (geocoords of Kassel)
            ),
            margin={"r":0,"t":50,"l":0,"b":0}, # define margin (top to 50 to display title)
            font = dict(
                size=text_size, # adjust font
                color="black"
            )
        )
        
        fig.update_geos( # adjust colors of the map
            showocean=True, oceancolor="#9EA7FF",
            showland=True, landcolor="#9EB965"
        )

        fig.update_traces(hovertemplate=hovertemplate) # apply hovertemplate

        for frame in fig.frames: # hovertemplate has to be applied for every animation frame
            frame.data[0].hovertemplate = hovertemplate

        fig.layout.sliders[0].currentvalue.prefix = slider_prefix  # set slider prefix (text above the slider)
            
        return fig  # return the created figure
    
    
    
class DynamicWeatherMapVisualizer(StaticWeatherMapVisualizer):
    """
    Can be used to visualize different types of dynamic weather maps, which are automatically updated with real-time data.
    Subclass of StaticWeatherMapVisualizer (inherits instance variables and methods). 
    Extends the StaticWeatherMapVisualizer with a method to update all created weather maps and overrides its get_dataframe()-method
    with a customized version of this method.
    """
    def __init__(self, kafka_con:KafkaReader):
        super().__init__() # call constructor of superclass so that its instance variables are created
        self.kafka_con = kafka_con # needs a kafka consumer/reader instance as instance variable (fetches only new data by making use of the offset)
    
    def get_dataframe(self) -> pd.DataFrame:
        """
        Fetches new data (from topic "weather.forecast.clean") by using the self.kafka_con instance.
        Prepares the new data the same way as it is done in the original get_dataframe()-method from StaticWeatherMapVisualizer.
        If it is the first time data has been fetched: Writes the data into self.df.
        If there is already data in self.df: Appends the newly fetched data to self.df.
        Then filters old forecasts and old timestamps from the whole dataframe self.df.
        Stores this dataframe in self.df AND returns it.
        """
        # fetch and prepare new data, for details: see get_dataframe()-method of StaticWeatherMapVisualizer
        data = self.kafka_con.retrieve()
        if data != []: # prepare data only if any data has been fetched (--> if list is not empty)
            data = pd.json_normalize(data)
            weather_data = data["weather"].apply(lambda x: x[0])
            data_weather = pd.json_normalize(weather_data)
            data = pd.concat((data, data_weather), axis=1)
            data = data.drop("weather", axis=1)
            data["temp_rounded"] = data["main.temp"].apply(lambda x: int(np.round(x, 0))).astype(str) + "°C"
            data["Date"] = data["dt_txt"].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M'))
            data["icon_symbol"] = data["icon"].apply(lambda x: icon_dict.get(x, ""))
            data["wind_symbol"] = data["wind.deg"].apply(lambda x: direction_dict.get(map_func(x), ""))
            data["wind_strength"] = data["wind.speed"].apply(lambda x: wind_strength_dict.get(map_func_speed(x), ""))
            data["wind"] = data["wind_symbol"] + data["wind_strength"]
            data["recommendation"] = data.apply(lambda x: add_recommendation(x), axis=1)

        # concat new and old data
        if type(data)==list and data == []: 
            pass # if no data has been fetched: do nothing here
        elif not self.df.empty: # if old dataframe is not empty: concatenate old and new data
            self.df = pd.concat((self.df, data), axis=0, ignore_index=True)
        else: # if old dataframe is empty: store fetched data in dataframe
            self.df = data

        # filter old forecast
        df_clean = pd.DataFrame().reindex(columns=self.df.columns)
        self.df["date_time_city"] = self.df["dt_txt"] + self.df["city.name"]
        for x in self.df["date_time_city"].unique():
            tmp = self.df.loc[self.df["date_time_city"]==x]
            latest_row = tmp.sort_values("fetched_at", ascending=False).iloc[0]
            df_clean.loc[len(df_clean)] = latest_row

        # filter data to current date --> drop all forecasts for time before now
        timestamp_now = datetime.timestamp(datetime.now())
        df_clean = df_clean.loc[df_clean["dt"]>timestamp_now]

        # fix dtypes
        df_clean["main.temp"] = df_clean["main.temp"].astype(float)
        df_clean["dt"] = df_clean["dt"].astype(float)

        self.df = df_clean # stores prepared dataframe in self.df
        
        return self.df # returns prepared dataframe
        
        
    # _create_map(), create_temperature_map(), create_icon_map() and create_wind_map() are directly inherited from superclass StaticWeatherMapVisualizer
    # as now changes must be made here
    
    
    def update_map_realtime(self): 
        """
        Updates all maps created with this class instance.
        Fetches new data from Kafka-Topic ("weather.forecast.clean") and updates each figure in self.list_of_figures with the newly fetched data.
        Clears old figures and shows the updated figures.
        """
        new_forecasts = self.get_dataframe() # update data
        clear_output(wait=True)  # clear old figures
        
        for i in range(len(self.list_of_figures)): # for each created figure in list_of_figures: update figure with new data
            fig_, typ = self.list_of_figures[i]
            
            
            if typ == "temp": # if figure is temperature map
                new_fig = px.scatter_geo(new_forecasts, 
                                        lat="city.coords.latitude",  # update with new temperature map data
                                        lon="city.coords.longitude", 
                                        color="main.temp", 
                                        scope="europe", 
                                        animation_frame="Date", 
                                        animation_group="city.name", 
                                        hover_name="city.name",
                                        hover_data=["city.name", "main.temp", "main.pressure", "main.humidity",
                                                    "description", "main.feels_like", "wind.speed", "wind.deg"],
                                        color_continuous_scale=cmap,
                                        width=800, height=600,
                                        text="temp_rounded",
                                        size=[20]*len(new_forecasts),
                                        title="5-day Temperature Forecast Germany"
                                )
                hovertemp = ('<b>%{customdata[0]}</b><br><br><i>%{customdata[4]}</i>'+ # set temperature hovertemplate
                             '<br><b>Temperature</b>: %{customdata[1]}°C (feels like: %{customdata[5]}°C)'+
                             '<br><b>Pressure</b>: %{customdata[2]} hPa<br><b>Humidity</b>: %{customdata[3]}%'+
                             '<br><b>Wind</b>: speed: %{customdata[6]} m/s, direction: %{customdata[7]}°')
                
            elif typ == "icon": # if figure is icon map
                new_fig = px.scatter_geo(new_forecasts, 
                                        lat="city.coords.latitude",  # update with new icon map data
                                        lon="city.coords.longitude", 
                                        scope="europe", 
                                        animation_frame="Date", 
                                        animation_group="city.name", 
                                        hover_name="city.name",
                                        hover_data=["city.name", "main.temp", "description", "recommendation"],
                                        color_continuous_scale=cmap,
                                        width=800, height=600,
                                        text="icon_symbol",
                                        size=[0]*len(new_forecasts),
                                        title="5-day Weather Forecast Germany<br><i>(hover over icons to see recommended actions)</i>"
                                )
                hovertemp = ('<b>%{customdata[0]}</b><br>'+ # set icon map hovertemplate
                             '<br><i>%{customdata[2]}</i>'+ 
                             '<br><b>Temperature</b>: %{customdata[1]}°C' + 
                             '<br><b>Recommendations</b>:<i>%{customdata[3]}</i>')
                
            elif typ == "wind": # if figure is wind map
                new_fig = px.scatter_geo(new_forecasts,  # update with new wind map data
                                        lat="city.coords.latitude", 
                                        lon="city.coords.longitude", 
                                        scope="europe", 
                                        animation_frame="Date", 
                                        animation_group="city.name", 
                                        hover_name="city.name",
                                        hover_data=["city.name", "wind.speed", "wind.deg"],
                                        color_continuous_scale=cmap,
                                        width=800, height=600,
                                        text="wind",
                                        size=[0]*len(new_forecasts),
                                        title="5-day Wind Forecast Germany"
                                )
                hovertemp = ('<b>%{customdata[0]}</b><br>'+ # set wind map hovertemplate
                             '<br><b>Wind Speed</b>: %{customdata[1]} m/s'+
                             '<br><b>Wind Direction</b>: %{customdata[2]}°')
            
            new_fig.update_traces(hovertemplate=hovertemp) # apply hovertemplate
            fig_.update_traces(new_fig.data[0]) # set figures data to updated data

            for frame in new_fig.frames: # update hovertemplate in each frame
                frame.data[0].hovertemplate = hovertemp
            
            fig_.frames = new_fig.frames # set figure frames to updated frames

            # adjust the slider if one frame is dropped because it lays in the past (e.g. 9:00 frame is dropped when its after 9:00)
            if datetime.strptime(fig_.layout.sliders[0].steps[0].label, '%d.%m.%Y %H:%M') < datetime.now():
                fig_.update_layout(sliders=[dict(steps=fig_.layout.sliders[0].steps[1:-1])])
            
            self.list_of_figures[i] = (fig_, typ) # update figure in self.list_of_figures
            fig_.show() # display the updated figure
            print('\n')
        