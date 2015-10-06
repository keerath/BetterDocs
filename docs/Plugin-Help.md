# KodeBeagle Plugin Help 
This document covers all the settings provided by the plugin.

### Identity
By choosing Opt-Out we don't track any search queries made to our server.

### Limits

1. **Lines from cursor** - This is used for widening the search context and selecting a higher count might not result in any imports.<br><br>

2. **Result Size** - This is used for increasing the number of results required per search.<br><br>

3. **Spotlight Result Count** - This is used for increasing the number of results in the spotlight section.<br><br>
**Note** - Selecting a higher spotlight count will result in a slow search.

### Configure Imports

By choosing to exclude imports, a user can configure the imports he wants to query. This can be done by providing a regex matching the import.

### Elastic Search Server

This allows to change the default elastic search server. To be overridden only if a user has his own elastic search server with all the required indexes.

### Notifications

To enable/disable kodebeagle notifications and logging.