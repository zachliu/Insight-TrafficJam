// Render the highlighted streets on Google Maps
var NY = new google.maps.LatLng(40.672622, -73.958389);
var map;
var roads = [];
var all_roads = [];
var REFRESH = 0;
var routetable = {};
var colortable = {};
var autotimer;

var Colors = [
    "#20CA20",    // green
    "#FF5800",    //
    "#F50000",    // red
    "#941313"     // dark red
];

function initialize() {
    var mapOptions = {
	zoom: 11,
	center: NY
    };
    map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);
    map.controls[google.maps.ControlPosition.TOP_RIGHT].push(new FullScreenControl(map));
    map.addListener('zoom_changed', function() {
        $.getJSON('/zoom_changed', {
            zoom: map.getZoom()
        }, function(data) {
            //$("#result").text(data.result);
            //alert(data.result);
        });
    });
}

function startStreaming() {
    if (!REFRESH) {
        REFRESH = 1;
        update_values_smart();
        alert("Streaming is started");
    }
}

function stopStreaming() {
    window.clearTimeout(autotimer);
    REFRESH = 0;
    alert("Streaming is stopped");
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

// the key idea is to only render the "changing" streets
function update_values_smart() {
    $.getJSON('/realtime_roads',
        function(data) {
            rds = data.roads;
            for (var i = 0; i < rds.length; i = i + 1) {
                var cc = rds[i].carcount;
                var stid = rds[i].name;
                var color_str = chooseColor(Number(cc));
                if (stid in routetable) {
                    if (colortable[stid] != color_str) {
                        routetable[stid].setOptions({strokeColor: color_str});
                        colortable[stid] = color_str;
                    }
                }
                else {
                    var highlight = [];
                    for (var j = 0; j < rds[i].coord.length; j = j + 1) {
                        highlight.push(new google.maps.LatLng(rds[i].coord[j][1], rds[i].coord[j][0]))
                    }
                    var route = new google.maps.Polyline({
                        path: highlight,
                        strokeColor: color_str,
                        strokeOpacity: 1,
                        strokeWeight: 3,
                        title: stid,
                        map: map
                    });
                    google.maps.event.addListener(route, "click", function(event) {
                        var base_url = "https://trafficjam.today/batch/";
                        var key = this.title;
                        window.open(base_url.concat(key));
                    });
                    routetable[stid] = route;
                    colortable[stid] = color_str;
                }
            }
        }
    );
    window.setTimeout(function(){ update_values_smart() }, 1500);
/*
    if (REFRESH) {
        autotimer = window.setTimeout(function(){ update_values_smart() }, 1000);
    }
*/
}

update_values_smart();

google.maps.event.addDomListener(window, 'load', initialize);
