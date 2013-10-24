var page = require('webpage').create(),
    address, output, size;

phantom.cookiesEnabled = true;

if (phantom.args.length < 2 || phantom.args.length > 3) {
    console.log('Usage: rasterize.js URL filename');
    phantom.exit();
} else {
    page.onError = function (msg, trace) {
        console.log(msg);
        trace.forEach(function(item) {
            console.log('  ', item.file, ':', item.line);
        });
    };
    address = phantom.args[0];
    output = phantom.args[1];
    //page.customHeaders = { 'Referer': address };
    page.viewportSize = { width: 1024, height: 728 };
    page.open(address, function (status) {
        if (status !== 'success') {
            console.error('Unable to load the address!');
        } else {
            //console.log(JSON.stringify(phantom.cookies, null, 2));
            window.setTimeout(function () {
                page.clipRect = page.evaluate(function() {
                    var cn = $('#centralNotice');

                    // workaround for broken banner css
                    var divHeight = cn.height();
                    if ( divHeight === 0 ) {
                        divHeight = page.viewportSize.height;
                        console.log("No height found, using default of " + divHeight);
                    }

                    return {
                        top: cn.offset().top,
                        left: cn.offset().left,
                        width: cn.width(),
                        height: divHeight
                    };
                });
                console.debug("#centralNotice was " + page.clipRect.width + "px x " + page.clipRect.height + "px");
                page.render(output);
                phantom.exit();
            }, 1000);
        }
    });
}
