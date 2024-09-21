To set up Google Drive access, you need to jump through many hoops.

#### Python dependencies
You need to have `pydrive` and `pycryptodome` installed.

#### Google Drive API setup
1. Go to https://console.developers.google.com
2. Set up a project. Give it whatever name you like.
3. On the left side of the screen, click on "Dashboard"
4. Click on "Enable APIs"
5. Find "Google Drive API", click on it, and click on "Enable"

#### Google Oauth Setup
1. On the same page, click on "Credentials" on the left.
2. Click "Create credentials" and select "OAuth client ID" from the drop-down menu
3. For "Application Type," choose "Other"
4. Give it whatever name you want
5. Click "Create"
6. On the next page, a message box will show you the Oauth ID and secrets. Dismiss the box.
7. On this page, download the client secrets file for the new Oauth client. (Right side of the screen). Save that to your ~/.config/cottoncandy/ folder with a name.
8. Edit your ~/.config/cottoncandy/options.cfg file and edit the "client_secrets" field to the name of the file you just downloaded.

#### First time-use
1. In python, do `cc.get_interface(backed = 'gdrive')`
2. When prompted, say yes to opening a local browser.
3. On the browser screen, choose your account, and allow access.
4. Once the page says something like "Authorization complete," you can close it and go back to python.
5. The cottoncandy object should be good to go.
