source = ["./dist/litestream"]
bundle_id = "com.middlemost.litestream"

apple_id {
  username = "benbjohnson@yahoo.com"
  password = "@env:AC_PASSWORD"
}

sign {
  application_identity = "Developer ID Application: Middlemost Systems, LLC"
}

zip {
  output_path = "dist/litestream.zip"
}
