// Render the markers for cab locations on Google Maps
var NY = new google.maps.LatLng(43.036821, -75.934096);
var markers = [];
var map;

function initialize() {
    var mapOptions = {
	zoom: 7,
	center: NY
    };
    map = new google.maps.Map(document.getElementById('map-canvas'),
			      mapOptions);
}

var DrivePath = [];
var roads = [];
var highlight = [];

var Colors = [
    "#20CA20",    // green
    "#B610FF",    // purple
    "#F50000",    // red
    "#941313"     // dark red
];

function update_values() {
    $.getJSON('/realtime',
        function(data) {
            cabs = data.cabs
            console.log(cabs)
            clearMarkers();
            for (var i = 0; i < cabs.length; i = i + 1) {
                addMarker(new google.maps.LatLng(cabs[i].lat, cabs[i].lng));
            }
            roads.push(new google.maps.Polyline({
                path: DrivePath,
                strokeColor: "#FF0000",
                strokeOpacity: 1.0,
                strokeWeight: 2,
                map: map,
            }));
        });
    window.setTimeout(update_values, 1000);
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

function update_values_road_test() {
    $.getJSON('/realtime_roads',
        function(data) {
            rds = data.roads
            clearRoads();
            for (var i = 0; i < rds.length; i = i + 1) {
                highlight = [];
                var cc = rds[i].carcount;
                for (var j = 0; j < rds[i].roadloc.length; j = j + 1) {
                    highlight.push(new google.maps.LatLng(rds[i].roadloc[j][1], rds[i].roadloc[j][0]))
                }
                roads.push(new google.maps.Polyline({
                    path: highlight,
                    strokeColor: chooseColor(Number(cc)),
                    strokeOpacity: 1,
                    strokeWeight: 5,
                    map: map,
                }));
            }
        });
    window.setTimeout(update_values_road_test, 5000);
}

//update_values();
update_values_road_test();

function drop(lat, lng) {
    point  = new google.maps.LatLng(lat,lng);
    clearMarkers();
    addMarker(point);
}

function addMarker(position) {
    markers.push(new google.maps.Marker({
	position: position,
	map: map,
    }));
    DrivePath.push(position);
}

function clearMarkers() {
    for (var i = 0; i < markers.length; i++) {
	markers[i].setMap(null);
    }
    for (var i = 0; i < roads.length; i++) {
	roads[i].setMap(null);
    }
    markers = [];
    DrivePath = [];
}

function clearRoads() {
    for (var i = 0; i < roads.length; i++) {
	roads[i].setMap(null);
    }
    //road = [];
}

google.maps.event.addDomListener(window, 'load', initialize);
