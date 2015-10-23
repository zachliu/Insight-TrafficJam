//alert("Cookie is here!");
function getCookie(cname) {
    var name = cname + "=";
    var ca = document.cookie.split(';');
    for(var i=0; i<ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0)==' ')
            c = c.substring(1);
        if (c.indexOf(name) == 0)
            return c.substring(name.length,c.length);
    }
    return "";
}

co = "";
//co = getCookie("session_id");
if (co == "") {
    //alert("no cookie");
    var d = new Date();
    var t = d.getTime();
    var val2 = parseInt(Math.random() * 10000) + "";
    var val1 = parseInt(t) + "";
    d.setTime(t + (0.5*24*60*60*1000));
    var expires = "expires="+d.toUTCString();
    document.cookie="session_id=" + val1 + val2 + "; " + expires  + "; path=/";
}
else {
    //alert(co);
}
