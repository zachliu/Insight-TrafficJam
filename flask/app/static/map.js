// Render the markers for cab locations on Google Maps
var NY = new google.maps.LatLng(43.036821, -75.934096);
var map;
var roads = [];
var all_roads = [];
var REFRESH = 0;

var Colors = [
    "#20CA20",    // green
    "#B610FF",    // purple
    "#F50000",    // red
    "#941313"     // dark red
];

function initialize() {
    var mapOptions = {
	zoom: 7,
	center: NY
    };
    map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);
/*
    $.getJSON('/all_roads',
        function(data) {
            net_layer = new google.maps.Data();
            net_layer.addGeoJson(data);
            net_layer.setMap(map);
            //map.data.addGeoJson(data);
        }
    );
*/

/*
    $.getJSON('/realtime_roads',
        function(data) {
            rds = data.roads
            clearRoads();
            for (var i = 0; i < rds.length; i = i + 1) {
                var highlight = [];
                var cc = rds[i].carcount;
                var stid = rds[i].name;
                for (var j = 0; j < rds[i].roadloc.length; j = j + 1) {
                    highlight.push(new google.maps.LatLng(rds[i].roadloc[j][1], rds[i].roadloc[j][0]))
                }
                var route = new google.maps.Polyline({
                    path: highlight,
                    strokeColor: chooseColor(Number(cc)),
                    strokeOpacity: 1,
                    strokeWeight: 5,
                    title: stid,
                    map: map
                });
                google.maps.event.addListener(route, "click", function(event) {
                    var base_url = "http://52.23.167.189/batch/";
                    var key = this.title;
                    window.open(base_url.concat(key));
                });
                roads.push(route);
            }
        }
    );
*/

}

function toggleNetwork() {
    if (REFRESH == 1) {
        alert("Stop refreshing");
        REFRESH = 0;
        update_values();
    }
    else {
        alert("Start refreshing");
        REFRESH = 1;
        update_values();
    }
}

function chooseColor(cc) {
    if (cc > 2000) {
        return Colors[3];
    }
    else if (cc > 1000) {
        return Colors[2];
    }
    else if (cc > 500) {
        return Colors[1];
    }
    else {
        return Colors[0];
    }
}


function update_values() {
    $.getJSON('/realtime_roads',
        function(data) {
            rds = data.roads
            clearRoads();
            for (var i = 0; i < rds.length; i = i + 1) {
                var highlight = [];
                var cc = rds[i].carcount;
                var stid = rds[i].name;
                for (var j = 0; j < rds[i].roadloc.length; j = j + 1) {
                    highlight.push(new google.maps.LatLng(rds[i].roadloc[j][1], rds[i].roadloc[j][0]))
                }
                var route = new google.maps.Polyline({
                    path: highlight,
                    strokeColor: chooseColor(Number(cc)),
                    strokeOpacity: 1,
                    strokeWeight: 5,
                    title: stid,
                    map: map
                });
                google.maps.event.addListener(route, "click", function(event) {
                    var base_url = "http://52.23.167.189/batch/";
                    var key = this.title;
                    window.open(base_url.concat(key));
                });
                roads.push(route);
            }
        }
    );
    if (REFRESH == 1) {
        window.setTimeout(update_values, 1500);
    }
    else {
        window.setTimeout(update_values, 3600000);
    }
}


update_values();


function clearRoads() {
    for (var i = 0; i < roads.length; i++) {
	roads[i].setMap(null);
    }
    roads = [];
}

google.maps.event.addDomListener(window, 'load', initialize);