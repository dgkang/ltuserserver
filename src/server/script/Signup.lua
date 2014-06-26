local argv = ARGV
local body = cjson.decode(argv[1])
body.time = argv[2]
body.code = 0
body.msg = "OK"
return cjson.encode(body)
