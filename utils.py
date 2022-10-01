# various functions and variables that are used by other classes


# self designed custom colormap for temperature
cmap = [[0.0, "rgb(6,1,63)"], 
        [0.1111111111111111, "rgb(6,6,246)"],
        [0.2222222222222222, "rgb(78,79,254)"],
        [0.3333333333333333, "rgb(147,149,251)"],
        [0.4444444444444444, "rgb(229,234,248)"],
        [0.5555555555555556, "rgb(250,245,113)"],
        [0.6666666666666666, "rgb(247,233,19)"],
        [0.7777777777777778, "rgb(255,193,3)"],
        [0.8888888888888888, "rgb(247,13,0)"],    
        [1.0, "rgb(81,0,1)"]]


# maps "icon"-column of data (id for OpenWeatherMap-Icon) to symbols
icon_dict = {
    "01d": "☀️", "01n": "🌙", #clear sky
    "02d": "⛅", "02n": "⛅", #few clouds
    "03d": "🌥️", "03n": "🌥️", #scattered clouds
    "04d": "☁️", "04n": "☁️", #broken clouds
    "09d": "🌧️", "09n": "🌧️", #shower rain
    "10d": "🌦️", "10n": "🌦️", #rain
    "11d": "🌩️", "11n": "🌩️", #thunderstorm
    "13d": "❄️", "13n": "❄️", #snow
    "50d": "🌫️", "50n": "🌫️" #mist 
}


# legend for icon weathermap:
icon_legend_text1 = "      ☀️clear sky (day)    🌙clear sky (night)    ⛅few clouds    🌥️scattered clouds    ☁️broken clouds"
icon_legend_text2 = "      🌧️shower rain         🌦️rain                      ❄️snow            🌩️thunderstorm        🌫️mist"


# maps wind speed x to a number between 0 and 4
def map_func_speed(x):
    if x < 3:
        return 0
    elif x < 8: 
        return 1
    elif x < 17:
        return 2
    elif x < 22:
        return 3
    else:
        return 4


# maps the wind stages 0-4 (result from function above) to a symbol
wind_strength_dict = {
    0: "🍃", # 0-3 m/s
    1: "🌬️", # 3-15 m/s
    2: "💨", # 15-25 m/s
    3: "🌪️", # 25-32 m/s
    4: "⚠️" # 32+ ms/s  -->approximation of Beaufort Skala for reference 
}


# legend for wind map
wind_legend_text1 = "               🍃calm/light breeze    🌬️gentle to strong breeze    💨near gale to strong gale"
wind_legend_text2 = "               🌪️(violent) storm       ⚠️hurricane                         ↗ wind direction"


# maps wind direction (x, in degree) to a number between 0-8 to approximate the direction with arrows (see below)
map_func = lambda x: int((x+22.5)/45)


# maps approximated wind direction (result from function above) to an arrow in the corresponding direction
direction_dict = {
    0: "⬆",
    1: "↗",
    2: "➡",
    3: "↘",
    4: "⬇",
    5: "↙",
    6: "⬅",
    7: "↖",
    8: "⬆"
}


# rounds the input to the nearest multiple of 5
def roundto5(x):
    return 5 * round(x/5)


# adds a recommendation for a forecast (x->row in dataframe) based on temperature, wind speed and description
def add_recommendation(x) -> str:
    rec_temp = recommendation_dict_temp.get(roundto5(x["main.temp"]), "") # get temperature recommendation for temperature rounded to nearest multiple of 5
    rec_wind = recommendation_dict_wind.get(map_func_speed(x["wind.speed"]), "") # get wind speed recommendation for mapped wind speed
    rec_descr = recommendation_dict_description.get(x["icon"], "") # get recommendation based on description/icon
    rec = f"{rec_temp}{rec_wind}{rec_descr}" # concatenate recommendation
    if rec and rec.strip():
        return rec # return recommendations if not empty
    return "-" # else return -
    
    
# maps temperature (rounded to nearest multiple of 5) to a recommendation for this temperature
recommendation_dict_temp = {
    -15: "<br>Wear thick gloves, warm jacket and shoes and winter hat.",
    -10: "<br>Wear gloves, warm jacket and winter hat.",
    -5: "<br>Wear gloves and warm jacket.",
    0: "<br>Be aware of icy, slippery ground!",
    5: "<br>Wear a warm jacket.",
    10: "<br>Wear a jacket.",
    15: "", # no special recommendation
    20: "<br>Wear a T-Shirt.",
    25: "<br>Take enough water with you!",
    30: "<br>Do not leave your dog in the car!", 
    35: "<br>Air your flat only in the morning and at night.",
    40: "<br>Close schools and unversities after lunch due to the heat.",
    45: "<br>Too hot for anything..."
}


# maps mapped wind speed to a recommendation for this wind strength
recommendation_dict_wind = {
    0: "", # no special recommendation
    1: "", 
    2: "<br>Be aware of flying branches!", 
    3: "<br>Do not leave the house!", 
    4: "<br>Do not leave the house and hide yourself in the basement!"
}


# maps icon (id from OpenWeatherMap) to a recommendation for this kind of weather
recommendation_dict_description = {
    "01d": "<br>Wear sunglasses and sunhat.", "01n": "<br>Watch and enjoy the clear night sky", #clear sky
    "02d": "<br>Perfect weather to walk the dog.", "02n": "",
    "03d": "<br>Perfect weather to walk the dog.", "03n": "", #scattered clouds
    "04d": "", "04n": "", # no special recommendation
    "09d": "<br>Use an umbrella and wear a rain jacket.", "09n": "<br>Use an umbrella and wear a rain jacket", #shower rain
    "10d": "<br>Take an umbrella with you and look for rainbows.", "10n": "<br>Take an umbrella with you.", #rain
    "11d": "<br>Watch out for lightnings. Stay at home or in your car.", "11n": "<br>Watch out for lightnings. Stay at home.", #thunderstorm
    "13d": "<br>Use winter tyres and build a snow man (and dog).", "13n": "<br>Use winter tyres and watch the snow falling.", #snow
    "50d": "<br>Switch on fog lights.", "50n": "<br>Switch on fog lights." #mist 
}