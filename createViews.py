import ViewHandler
viewHandler = ViewHandler.ViewHandler("config_v2.xml", {
        "host": "localhost",
        "user": "root",
        "passwd": "sk@7NFJz",
        "database": "stdwh_db",
        "charset": 'utf8',
})
viewHandler.createViews()
viewHandler.createBufferFactTable()