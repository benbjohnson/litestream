source = ["./dist/litestream"]
bundle_id = "com.middlemost.litestream"

apple_id {
  username = "@env:APPLE_ID_USERNAME"
  password = "@env:AC_PASSWORD"
  provider = "@env:APPLE_TEAM_ID"
}

sign {
  application_identity = "@env:APPLE_DEVELOPER_ID_APPLICATION"
  entitlements_file = ""
}

notarize {
  path = "./dist/litestream.zip"
  bundle_id = "com.middlemost.litestream"
  staple = true
}

zip {
  output_path = "./dist/litestream-signed.zip"
}
