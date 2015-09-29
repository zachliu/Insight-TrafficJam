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

var DrivePath = [
/*
  new google.maps.LatLng(37.772323, -122.214897),
  new google.maps.LatLng(21.291982, -157.821856),
  new google.maps.LatLng(-18.142599, 178.431),
  new google.maps.LatLng(-27.46758, 153.027892),
  new google.maps.LatLng(12.97918167,   77.6449),
  new google.maps.LatLng(12.97918667,   77.64487167),
  new google.maps.LatLng(12.979185, 77.64479167),
  new google.maps.LatLng(12.97918333, 77.64476)
*/
];

var roads = [];

/*
var PathStyle = new google.maps.Polyline({
  path: DrivePath,
  strokeColor: "#FF0000",
  strokeOpacity: 1.0,
  strokeWeight: 2
});
*/
//PathStyle.setMap(map);

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
    //PathStyle.setMap(map);
}
update_values();
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
    //roads[0].setMap(null);
    markers = [];
    //PathStyle.setMap(null);
    DrivePath = [];
}
google.maps.event.addDomListener(window, 'load', initialize);
