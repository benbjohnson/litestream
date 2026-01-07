# WebDAV Configuration

Replicate SQLite databases to WebDAV-compatible servers.

## Quick Start

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: webdav
      webdav-url: https://webdav.example.com/backups/app
      webdav-username: ${WEBDAV_USER}
      webdav-password: ${WEBDAV_PASS}
```

## Configuration

WebDAV requires explicit type declaration:

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: webdav
      webdav-url: https://server.example.com/dav/backups
      webdav-username: litestream
      webdav-password: ${WEBDAV_PASSWORD}
```

## Configuration Options

| Option | Description |
|--------|-------------|
| `webdav-url` | WebDAV server URL including path |
| `webdav-username` | Authentication username |
| `webdav-password` | Authentication password |

## Examples

### NextCloud

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: webdav
      webdav-url: https://nextcloud.example.com/remote.php/dav/files/username/Backups/app
      webdav-username: username
      webdav-password: ${NEXTCLOUD_APP_PASSWORD}
```

**Note**: Use an app password, not your main NextCloud password.

### ownCloud

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: webdav
      webdav-url: https://owncloud.example.com/remote.php/webdav/Backups/app
      webdav-username: username
      webdav-password: ${OWNCLOUD_PASSWORD}
```

### Apache mod_dav

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: webdav
      webdav-url: https://webdav.example.com/backups/app
      webdav-username: davuser
      webdav-password: ${WEBDAV_PASSWORD}
```

### Nginx with ngx_http_dav_module

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: webdav
      webdav-url: https://dav.example.com/backups/app
      webdav-username: backup
      webdav-password: ${DAV_PASSWORD}
```

### Box.com

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: webdav
      webdav-url: https://dav.box.com/dav/Backups/app
      webdav-username: your-email@example.com
      webdav-password: ${BOX_PASSWORD}
```

## Server Setup

### Apache mod_dav

```apache
<VirtualHost *:443>
    ServerName webdav.example.com
    DocumentRoot /var/www/webdav

    <Directory /var/www/webdav>
        Dav On
        AuthType Basic
        AuthName "WebDAV"
        AuthUserFile /etc/apache2/.htpasswd
        Require valid-user

        Options +Indexes
        DirectoryIndex disabled
    </Directory>
</VirtualHost>
```

Create user:
```bash
htpasswd -c /etc/apache2/.htpasswd davuser
```

### Nginx

```nginx
server {
    listen 443 ssl;
    server_name dav.example.com;

    root /var/www/webdav;

    location / {
        dav_methods PUT DELETE MKCOL COPY MOVE;
        dav_ext_methods PROPFIND OPTIONS;

        auth_basic "WebDAV";
        auth_basic_user_file /etc/nginx/.htpasswd;

        client_max_body_size 100M;
        create_full_put_path on;
        dav_access user:rw group:rw all:r;
    }
}
```

## Troubleshooting

### "401 Unauthorized"
- Verify username and password are correct
- Check authentication is enabled on server
- For NextCloud/ownCloud, use app passwords

### "404 Not Found"
- Verify the URL path is correct
- Ensure parent directories exist
- Check URL includes full path to backup location

### "403 Forbidden"
- Verify user has write permissions
- Check WebDAV is enabled for the location
- Review server access control settings

### "Connection refused"
- Verify server is running
- Check SSL certificate is valid
- Ensure firewall allows HTTPS

### Slow transfers
- WebDAV has higher overhead than native protocols
- Consider using S3 or SFTP for better performance
- Check server's upload size limits
