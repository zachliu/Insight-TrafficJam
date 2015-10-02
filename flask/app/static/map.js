// Render the markers for cab locations on Google Maps
var NY = new google.maps.LatLng(43.036821, -75.934096);
var map;
var roads = [];
var all_roads = [];

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

    google.maps.event.addListener(map, "rightclick", function(event) {
        var lat = event.latLng.lat();
        var lng = event.latLng.lng();
        // populate yor box/field with lat, lng
        alert("Lat=" + lat + "; Lng=" + lng);
    });
    $.getJSON('/all_roads',
        function(data) {
            rds = data.roads
            for (var i = 0; i < rds.length; i = i + 1) {
                var highlight = [];
                var stid = rds[i].name;
                for (var j = 0; j < rds[i].roadloc.length; j = j + 1) {
                    highlight.push(new google.maps.LatLng(rds[i].roadloc[j][1], rds[i].roadloc[j][0]))
                }
                var route = new google.maps.Polyline({
                    path: highlight,
                    strokeColor: "#989899",
                    strokeOpacity: 0.7,
                    strokeWeight: 2,
                    title: stid,
                    map: map
                });
                google.maps.event.addListener(route, "click", function(event) {
                    //var lat = event.latLng.lat();
                    //var lng = event.latLng.lng();
                    // populate yor box/field with lat, lng
                    //alert("Lat=" + lat + "; Lng=" + lng);
                    //alert(this.title)
                    var base_url = "http://54.174.177.48/batch/";
                    var key = this.title;
                    window.open(base_url.concat(key));
                });
                //all_roads.push(route);
            }
        }
    );
}

/*
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
*/

/*
function update_values_road_test() {
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
                    //var lat = event.latLng.lat();
                    //var lng = event.latLng.lng();
                    // populate yor box/field with lat, lng
                    //alert("Lat=" + lat + "; Lng=" + lng);
                    //alert(this.title)
                    var base_url = "http://54.174.177.48/batch/";
                    var key = this.title;
                    window.open(base_url.concat(key));
                });
                roads.push(route);
            }
        }
    );
    window.setTimeout(update_values_road_test, 2500);
}
*/

//update_values();
//update_values_road_test();

/*
function clearRoads() {
    for (var i = 0; i < roads.length; i++) {
	roads[i].setMap(null);
    }
    roads = [];
}
*/

google.maps.event.addDomListener(window, 'load', initialize);
