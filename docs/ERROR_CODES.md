# Error Codes Reference

This document lists all error codes that may appear when using CASS (Coding Agent Session Search) and provides guidance for resolution.

## Error Code Format

Error codes follow the format `E<category><number>`:
- **E1xx**: Authentication errors
- **E2xx**: Archive format errors
- **E3xx**: Database errors
- **E4xx**: Browser compatibility errors
- **E5xx**: Network errors
- **E6xx**: Export errors

## Authentication Errors (E1xx)

### E101: Authentication Failed

**Message**: "The password you entered is incorrect."

**Cause**: The provided password does not match the archive's encryption key.

**Resolution**:
- Double-check your password (passwords are case-sensitive)
- Ensure you're using the password set when the archive was created
- If you've forgotten the password, use your recovery key if available

### E102: Empty Password

**Message**: "Please enter a password."

**Cause**: The password field was left empty.

**Resolution**: Enter your password before clicking "Unlock".

### E103: No Matching Key Slot

**Message**: "No valid key found for this archive."

**Cause**: The provided credentials don't match any encryption slot in the archive.

**Resolution**:
- Try your password again
- If using a recovery key, ensure it's the correct one for this archive
- The archive may have been re-encrypted with different credentials

## Archive Format Errors (E2xx)

### E201: Invalid Archive Format

**Message**: "This doesn't appear to be a valid CASS archive."

**Cause**: The file is not a recognized CASS archive format, or has been modified.

**Resolution**:
- Verify you're opening the correct file
- Try downloading the archive again
- Check that the file hasn't been modified or corrupted during transfer

### E202: Integrity Check Failed

**Message**: "The archive appears to be corrupted or tampered with."

**Cause**: The archive's cryptographic integrity verification failed, indicating data corruption or modification.

**Resolution**:
- Download the archive again from the original source
- Check that the file transferred completely
- The archive may have been damaged during storage

### E203: Unsupported Version

**Message**: "This archive was created with a newer version of CASS."

**Cause**: The archive version is newer than the viewer can handle.

**Resolution**:
- Update to the latest version of CASS
- Check the CASS releases page for updates

### E204: Crypto Error

**Message**: "An encryption error occurred."

**Cause**: The cryptographic operation failed unexpectedly.

**Resolution**:
- Try again - this may be a transient error
- If persisting, download the archive again
- Report the issue if it continues

## Database Errors (E3xx)

### E301: Corrupt Database

**Message**: "The archive's internal database is corrupted."

**Cause**: The SQLite database inside the archive is damaged.

**Resolution**:
- Download the archive again
- Re-export from the original source if available
- The archive may have been damaged during creation

### E302: Missing Data

**Message**: "Required data is missing from this archive."

**Cause**: The archive is incomplete or was created with an incompatible version.

**Resolution**:
- Re-export from the original CASS database
- Ensure you're using a compatible version of CASS

### E303: Search Query Error

**Message**: "Your search could not be processed."

**Cause**: The search query contains syntax that cannot be interpreted.

**Resolution**:
- Simplify your search query
- Remove special characters
- Use quotes around phrases

### E304: Database Locked

**Message**: "The database is temporarily unavailable."

**Cause**: Another operation is currently accessing the database.

**Resolution**:
- Wait a moment and try again
- Close other browser tabs viewing the same archive
- Refresh the page

### E305: No Results

**Message**: "No matching conversations found."

**Cause**: The search returned no results.

**Resolution**:
- Try different search terms
- Check your filter settings
- Broaden your date range if filtering by date

## Browser Errors (E4xx)

### E401: Unsupported Browser

**Message**: "Your browser doesn't support a required feature."

**Cause**: The browser is missing Web Crypto API, IndexedDB, or other required APIs.

**Resolution**:
- Use a modern browser: Chrome 90+, Firefox 90+, Safari 15+, Edge 90+
- Update your browser to the latest version
- Disable privacy extensions that may block required features

### E402: WebAssembly Not Supported

**Message**: "Your browser doesn't support WebAssembly."

**Cause**: WebAssembly is not available, possibly due to browser settings or version.

**Resolution**:
- Update your browser to a recent version
- Check that JavaScript is enabled
- Disable extensions that may block WebAssembly

### E403: Cryptography Not Available

**Message**: "Secure encryption is not available in your browser."

**Cause**: The Web Crypto API is not available, possibly due to insecure context (HTTP).

**Resolution**:
- Access the archive via HTTPS
- Serve the file from a local web server (not `file://`)
- Use a supported browser

### E404: Storage Quota Exceeded

**Message**: "Browser storage is full."

**Cause**: The browser's storage quota has been exceeded.

**Resolution**:
- Clear browser data for the site
- Close other tabs viewing large archives
- Increase storage allocation in browser settings

### E405: Cross-Origin Isolation Required

**Message**: "This feature requires cross-origin isolation."

**Cause**: SharedArrayBuffer is required but not available due to missing COOP/COEP headers.

**Resolution**:
- Serve the archive from a properly configured web server
- Contact the site administrator about enabling required headers

## Network Errors (E5xx)

### E501: Download Failed

**Message**: "Could not download the archive."

**Cause**: The network request to fetch the archive failed.

**Resolution**:
- Check your internet connection
- Try again in a few moments
- Verify the archive URL is correct

### E502: Incomplete Download

**Message**: "The download was incomplete."

**Cause**: The file was only partially downloaded.

**Resolution**:
- Try downloading again
- Check your internet connection stability
- Clear browser cache and retry

### E503: Request Timed Out

**Message**: "The request timed out."

**Cause**: The server took too long to respond.

**Resolution**:
- Try again later
- Check server status
- The archive may be too large for the current connection

### E504: Server Error

**Message**: "The server returned an error."

**Cause**: The web server returned an error status code.

**Resolution**:
- Try again later
- Check that the archive URL is correct
- Contact the server administrator

## Export Errors (E6xx)

### E601: No Conversations to Export

**Message**: "There are no conversations to export."

**Cause**: The source database contains no conversations.

**Resolution**:
- Check that CASS has indexed some conversations
- Run `cass index` to scan for new conversations
- Verify the correct database path

### E602: Source Database Error

**Message**: "Could not read the source database."

**Cause**: The CASS database could not be opened or read.

**Resolution**:
- Verify the database path is correct
- Check file permissions
- Run `cass health` to diagnose issues

### E603: Output Error

**Message**: "Could not write the output file."

**Cause**: The export file could not be written.

**Resolution**:
- Check the output directory exists
- Verify write permissions
- Ensure sufficient disk space

### E604: Filter Matched Nothing

**Message**: "Your filters didn't match any conversations."

**Cause**: The export filters excluded all conversations.

**Resolution**:
- Broaden your filter criteria
- Check agent and workspace filters
- Expand the date range

## Getting Help

If you encounter an error not listed here or need additional assistance:

1. **Check the logs**: Run with `--verbose` for detailed output
2. **Search existing issues**: https://github.com/anthropics/cass/issues
3. **File a new issue**: Include the error code, message, and steps to reproduce

## Reporting Bugs

When reporting an error, please include:

- Error code and message
- CASS version (`cass --version`)
- Browser and version (for web viewer)
- Operating system
- Steps to reproduce
- Any relevant log output

Do NOT include:
- Passwords or recovery keys
- Personal conversation content
- Sensitive file paths
