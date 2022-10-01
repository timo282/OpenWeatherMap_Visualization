# Big Data Programming - Visualization of OpenWeatherMap Data

## Part I:

*Fetching von Sensordaten/Vorhersage-Daten (https://openweathermap.org/forecast5) und "buffern" in Kafka. Daten sollen häufiger gelesen werden, als der Sensor seine Messdaten intervallbasiert aktualisiert (möglichen Service-Ausfall kompensieren).*


Diese Aufgabe wurde in *CollectAndCleanData.ipynb* gelöst.

##### Variablen und Funktionen:
<table>
    <thead>
        <th>Typ</th><th>Name</th><th>Beschreibung</th>
    </thead>
    <tbody>
        <tr><td>Variable</td><td>openWeatherMap</td><td>OpenWeatherMap-Instanz mit spezifiziertem API-Key</td></tr>
        <tr><td>Variable</td><td>kafka_prod1</td><td>KafkaWriter-Instanz als Kafka-Produzent</td></tr>
        <tr><td>Funktion</td><td>load_locations()</td><td>Lädt die JSON mit Orten und gibt diese zurück</td></tr>
        <tr><td>Funktion</td><td>collect_forecast_data()</td><td>Fragt für jeden Ort die 5-Tage-Vorhersage von OpenWeatherMap ab und speichert die zurückgegebenen Werte im Kafka-Topic weather.forecast.raw</td></tr>
    </tbody>
</table>

##### Erläuterung, Zusammenhänge und Abhängigkeiten:
In einer Endlos-Schleife wird die Funktion collect_forecast_data() alle 15 Minuten aufgerufen (clean_forecast_data() kann für diese Aufgabe ignoriert werden). Zunächst werden mit der Funktion load_locations() die Orte geladen. Anschließend wird durch diese Orte iteriert. Für jeden Ort geschieht folgendes: Die 5-Tage Vorhersage für diesen Ort wird von OpenWeatherMap abgefragt mithilfe der get_forecast()-Methode der OpenWeatherMap-Klasse (bereits vorgegeben). Für jedes Element der Vorhersage ("für jeden Zeitpunkt") passiert folgendes: Zum Vorhersage-Dictionary wird der Ort mitsamt Geokoordinaten hinzugefügt und der aktuelle Zeitstempel ergänzt. Dies wird nun als Message mit zufällig generierter UUID als key durch den Kafka-Produzent in das Kafka-Topic "weather.forecast.raw" geschrieben. Hierzu wird der Kafka-Reader kafka_prod1 und dessen store()-Methode verwendet (Implementierung bereits vorgegeben).

## Part II:

*Aufgrund der Ausfallkompensation treten Dubletten von Messwerten auf. Diese sollen in der Streaming-Pipeline erkannt werden. In einem separaten Topic sollen die "gecleanten" Werte abgespeichert werden.*

Diese Aufgabe wurde ebenfalls in *CollectAndCleanData.ipynb* gelöst.

<table>
    <thead>
        <th>Typ</th><th>Name</th><th>Beschreibung</th>
    </thead>
    <tbody>
        <tr><td>Variable</td><td>kafka_con1</td><td>KafkaReader-Instanz, die vom Topic "weather.forecast.raw" liest</td></tr>
        <tr><td>Variable</td><td>kafka_prod2</td><td>KafkaWriter-Instanz, die ins Topic "weather.forecast.clean" schreibt</td></tr>
        <tr><td>Funktion</td><td>clean_forecast_data()</td><td>Liest neue Rohprognosedaten aus "weather.forecast.raw", vergleicht sie mit den Daten, die bereits in "weather.forecast.clean" gespeichert sind und fügt fehlende Daten zum "bereinigten" Thema hinzu. Dieser Prozess führt dazu, dass das "bereinigte" Topic keine Dubletten enthält.</td></tr>
    </tbody>
</table>

##### Erläuterung, Zusammenhänge und Abhängigkeiten:
In der oben beschriebenen Endlos-Schleife wird nun nach Aufruf der collect_forecast_data()-Funktion auch die clean_forecast_data()-Funktion alle 15 Minuten aufgerufen. Letztere Funktion ist für die Bereinigung der Daten von Dubletten zuständig. Der KafkaReader kafka_con1 liest die seid dem letzten Aufruf hinzugekommenen Messages aus dem Topic "weather.forecast.raw". Eine andere Instanz eines KafkaReaders (wird bei jedem Aufruf neu instanziiert) liest nun die bereits bereinigten Daten aus dem Topic "weather.forecast.clean". Jedes Element aus den neuen Rohdaten wird nun mit den bereits bereinigten Daten verglichen mithilfe der check_if_duplicate_in_list()-Funktion aus CheckDuplicates.py. Ein Element ist Dublette (d.h. bereits in den bereinigten Daten), wenn es bis auf den fetch-Zeitstempel mit einem anderen Element der bereits bereinigten Daten übereinstimmt. Alle neuen Daten werden dann jeweils in einer Message mit zufälliger UUID als Key vom Produzenten kafka_prod2 ins Topic "weather.forecast.clean" geschrieben. Somit ist das Topic "weather.forecast.clean" dublettenfrei.

**Hinweis:** *Als Dubletten werden hier nur Elemente gezählt, die 1zu1 (abgesehen vom fetch-Zeitstempel) übereinstimmen. Das heißt, Elemente die veraltet sind, weil es inzwischen eine neuere Vorhersage gibt, werden hier nicht herausgefiltert. Diese Aufgabe obliegt dem jeweiligen Konsumenten. Hintergedanke ist, dass ein anderer Service mit anderem Konsumenten vielleicht die "Entwicklung" einer Vorhersage darstellen möchte (d.h. wie die Vorhersage genauer wird je näher man dem Zeitpunkt des Eintreffens kommt).*

## Part III:

*Erstellung eines Microservice, welcher eine Infographik zur Verfügung stellt um die Messwerte darzustellen.*

Für diese Aufgabe muss *ShowData_01.ipynb* ausgeführt werden. Die Grafiken werden dann in diesem Notebook angezeigt.

##### Libraries

<table>
    <thead>
        <th>Library</th><th>Einsatz</th>
    </thead>
    <tbody>
        <tr><td>datetime</td><td>Verarbeitung von Datumswerten</td></tr>
        <tr><td>pandas</td><td>Speicherung und Verarbeitung der Daten in DataFrames</td></tr>
        <tr><td>plotly.express</td><td>Darstellung der Wetterkarten als Grafik (scatter_geo)</td></tr>
        <tr><td>uuid</td><td>Zufällige Generierung einzigartiger Hex-Keys</td></tr>
        <tr><td>numpy</td><td>Verwendung verschiedener mathematischer Funktionen (bspw. round)</td></tr>
    </tbody>
</table>

##### Wie kann Ihre Infographik vom Benutzer gelesen werden?

*Temperatur-Karte*<br>
Die Orte, für die eine Vorhersage gezeigt wird, sind als farbige Punkte dargestellt. Die Farbe ist dabei in Abhängigkeit der vorhergesagten Temperatur gewählt, von dunkelrot (heiß) bis dunkelblau (kalt). Diese Temperaturabstufung ist neben der Karte in einer Colorbar dargestellt. So kann der Benutzer auf den ersten Blick besonders heiße (oder kalte) Vorhersagen erkennen. Die farbigen Punkte sind zusätzlich mit dem vorhergesagten Temperaturwert beschriftet. Der Benutzer kann darüber hinaus mit dem Mauszeiger über die Punkte hovern, um zusätzliche Informationen zu erhalten (bspw. Luftfeuchtigkeit, Wetterbeschreibung, etc.). Um den Zeitpunkt der Vorhersage zu verändern, kann entweder der Slider unter der Grafik an die passende Position gezogen werden oder die zeitliche Entwicklung kann mithilfe des Play-Buttons abgespielt werden (Stopp-Button um wieder anzuhalten). Datum und Uhrzeit der aktuell dargestellten Vorhersage sind dabei über dem Slider gezeigt.<br>

*Icon-Karte*<br>
Diese Karte ist ähnlich aufgebaut wie die Temperaturkarte. Unterschied ist hier jedoch, dass anstelle der farbigen Temperaturpunkte nun Icons für die Orte dargestellt sind. Diese bauen auf der Beschreibung auf und visualisieren diese anschaulich (bewölkt, Regen, Schnee, etc.). Die Bedeutung der Icons ist in einer Legende über dem Slider gezeigt. Darüber hinaus kann der Benutzer auch hier über die Icons hovern. Nun erscheint neben der deutschen Beschreibung und der Temperatur auch eine "Handlungsempfehlung" für den Benutzer. Diese werden basierend auf Temperatur, Windstärke und eben der Beschreibung generiert.

*Wind-Karte*<br>
Auch diese Karte ist ähnlich aufgebaut wie die Vorherigen. Nun wird jedoch der Wind dargestellt. Zum einen wird die ungefähre Windrichtung für jede Ortschaft durch Pfeile dargestellt. Zusätzlich zeigt die Karte die Stärke des Windes (Windgeschwindigkeit), visualisiert durch ein entsprechendes Symbol. Die Symbole können aus der Legende über dem Slider abgelesen werden. Für detailliertere Informationen sind genaue Windrichtung und -geschwindigkeit durch Hovern über den entsprechenden Ort sichtbar.

##### Erläuterung zum Code:

Zur Visualisierung von statischen (nicht automatisch aktualisierenden) Wetterkarten wird die selbst geschriebene Klasse StaticWeatherMapVisualizer in visualization.py verwendet. Für jede der drei Grafik-Arten (s.o.) steht eine Methode zur Verfügung, um eine entsprechende Grafik zu erstellen. Da sich alle drei Wetterkarten bei der Erstellung nur in wenigen Details unterscheiden, basieren alle drei Visualierungs-Methoden auf derselben Basismethode (\_create\_map()). In dieser Methode werden zunächst, wenn nicht bereits im Zuge der Erstellung einer anderen Grafik geschehen, die Daten aus dem Topic "weather.forecast.clean" mithilfe eines Kafka-Konsumenten ausgelesen und in ein DataFrame geschrieben durch Aufruf der Methode get_dataframe() (+anschließende Vorverarbeitung). Nun wird mit diesen Daten eine Figure erzeugt mithilfe der plotly.express-Funktion scatter_geo(). Nach weiteren Anpassungen am Layout der Grafik wird die erzeugte Figure-Instanz zurückgegeben. Diese \_create\_map()-Methode wird in allen drei Visualisierungsmethoden aufgerufen. Jeweils werden als Methoden-Parameter die Werte übergeben, die für den jeweiligen Grafiktyp notwendig sind (z.B. u.a. color="main.temp" für Temperaturkarte). Die Visualisierungsmethoden stellen dann die erzeugte Figure im Output der Jupyter Notebook-Zelle dar (mithilfe der show()-Methode).

## Part IV:

*Anpassung der Infographik, dass sich die Werte bei einer Erneuerung der Messung (neue Message in Kafka) automatisch aktualisieren und so der Infografik immer die aktuellen Daten zur Verfügung stehen.*

Für diese Aufgabe muss *ShowData_02.ipynb* ausgeführt werden. Die Grafiken werden dann in diesem Notebook angezeigt und solange das Notebook läuft auch regelmäßig aktualisiert.

##### Libraries

Zusätzlich zu den Libraries aus Aufgabe 5 wird verwendet:
<table>
    <thead>
        <th>Library</th><th>Einsatz</th>
    </thead>
    <tbody>
        <tr><td>IPython.display</td><td>clear_output() zum Bereinigen des Outputs im Jupyter-Notebook</td></tr>
    </tbody>
</table>

##### Änderungen im Vergleich zu A5
Im Vergleich zu den statischen Wetterkarten in Aufgabe 5, müssen für die dynamischen (automatisch aktualisierenden) Wetterkarten einige wenige Erweiterungen vorgenommen werden. Hierfür bietet sich aus Implementierungssicht das Konzept der Vererbung an: Die nun verwendete Klasse DynamicWeatherMapVisualizer erweitert die Klasse StaticWeatherMapVisualizer (d.h. StaticWeatherMapVisualizer ist Superklasse). Dadurch werden die Instanzvariablen und Methoden der Superklasse geerbt. Die meisten davon können ohne zusätzliche Anpassungen verwendet werden. So müssen für Aufgabe 6 nur noch zwei Methoden geschrieben bzw. angepasst werden. Zum einen wird die get_dataframe()-Methode überschrieben. Sie verwendet nun eine feste Kafka-Konsumenten-Instanz, die immer beim letzten Offset weiterliest und so pro Aufruf nur neu hinzugekommene Messages konsumiert. Die neu ausgelesenen Daten werden dann an das DataFrame mit den bisher geholten Daten angehängt (+anschließende Vorverarbeitung). Zum anderen wird eine neue Methode benötigt, die die Wetterkarten updatet. Die Methode update_map_realtime() nutzt nun die angepasste get_dataframe()-Methode um die aktuellen Daten zu erhalten. Mit diesen Daten updatet sie alle Grafiken, die von der Klasse erstellt wurden. Um nun realtime Grafiken zu erstellen, wird die Klasse DynamicWeatherMapVisualizer folgendermaßen genutzt: Zunächst wird eine Instanz der Klasse erzeugt und mit den jeweiligen create_map_...()-Methoden werden Wetterkarten erzeugt. In einer Endlos-Schleife werden dann in einem gewissen Intervall (hier z.B. alle 15 Minuten, selbes Intervall wie Data Collection) alle erstellten Wetterkarten aktualisiert mit update_map_realtime().

##### Vorteile des objektorientierten Aufbaus des Codes für A5 und A6

Der objektorientierte Aufbau des Quellcodes für die statischen und dynamischen Visualisierungen erlaubt auch die gleichzeitige Erstellung und Aktualisierung mehrerer Wetterkarte (z.B. 2 Temperaturkarten in derselben Klasse). Außerdem können auch andere Services erstellt werden, die die Klassen nutzen können, um selbst entsprechende Wetterkarten darzustellen (z.B. ein Service nur für Temperaturkarte und einer für Windkarte, o.ä.). Zuletzt können auch ohne viel Aufwand neue Arten von Wetterkarten hinzugefügt werden, wenn bestehende Struktur und Basismethoden (wie \_create\_map()) genutzt werden.

## Abschließende allgemeine Hinweise:
Zur kontinuierlichen Abfrage der Daten von OpenWeatherMap und deren Bereinigung um Dubletten (Aufgabe 3 und 4) muss das Notebook *CollectAndCleanData.ipynb* laufen. Zur Darstellung der statischen Wetterkarten (Aufgabe 5) muss das Notebook *ShowData_01.ipynb* (einmalig) ausgeführt werden. Für die dynamischen (real-time) Wetterkarten (Aufgabe 6) muss das Notebook *ShowData_02.ipynb* laufen (für Aktualisierung der Daten sollte dann auch *CollectAndCleanData.ipynb* parallel laufen).
<br>
<br>
Für genauere Beschreibungen der Implementierung sei auf den ausführlich kommentierten Quellcode verwiesen.
