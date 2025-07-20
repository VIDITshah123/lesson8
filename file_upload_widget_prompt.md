
# Prompt: Build a Reusable File Upload Widget

## 1. High-Level Goal

Create a self-contained, reusable file upload widget that can be easily integrated into any web application. The widget's behavior and appearance should be configurable through a dedicated settings page.

## 2. Core Components

The solution requires two main parts:

1.  **The File Upload Widget (Frontend):** A UI component that users interact with to select and upload files.
2.  **The Configuration Page (Frontend/Backend):** A settings page where a developer can customize the widget's properties.

## 3. Detailed Requirements

### 3.1. The Configuration Page

This page allows a developer to define the behavior of a specific instance of the file upload widget. It should be a simple, user-friendly form with the following settings:

*   **UI Label (Text Input):**
    *   **Purpose:** To define the user-facing label for the file input field (e.g., "Upload Your Resume", "Attach Images", "Select a Document").
    *   **Validation:** Cannot be empty.

*   **Upload Storage Location (Text Input):**
    *   **Purpose:** To specify the absolute server-side directory path where the uploaded files should be stored (e.g., `/uploads/resumes/`, `/assets/user_images/`).
    *   **Validation:** Must be a valid-looking directory path. The backend should create this directory if it doesn't exist.

*   **Allow Multiple Files (Checkbox/Toggle):**
    *   **Purpose:** To control whether the user can select and upload one file at a time or multiple files simultaneously.
    *   **Default:** Unchecked (single file upload).

*   **File Name Prefix (Text Input):**
    *   **Purpose:** To specify an optional string to be prepended to the name of every file uploaded through this widget. For example, if the prefix is `invoice_` and the user uploads `report.pdf`, the file should be saved as `invoice_report.pdf`.
    *   **Validation:** Can be empty. Should only allow alphanumeric characters, underscores, and hyphens.

*   **Saving Configuration:**
    *   There must be a "Save Configuration" button.
    *   When clicked, the settings should be saved in a persistent format (e.g., a JSON file on the server like `widget-config.json`, or a database table).

### 3.2. The File Upload Widget

This is the actual UI component that will be displayed in the main application.

*   **Appearance:**
    *   It should display the custom **UI Label** defined in the configuration.
    *   It should include a standard file input button and/or a drag-and-drop area.
    *   It should clearly indicate whether single or multiple file uploads are allowed, based on the configuration.

*   **Functionality:**
    *   When a user selects file(s), the widget should display the names of the selected files.
    *   An "Upload" button should trigger the upload process.
    *   The widget must communicate with a dedicated backend endpoint to handle the file transfer.

### 3.3. Backend API

A backend endpoint is required to handle the file uploads.

*   **Endpoint:** Create a single, robust endpoint (e.g., `POST /api/upload`).
*   **File Handling:**
    *   The endpoint must read the saved configuration to determine the correct storage location and file prefix.
    *   It must apply the **File Name Prefix** to each file before saving it to the specified **Upload Storage Location**.
    *   It should handle potential errors gracefully (e.g., file size limits, invalid file types, storage permission issues) and return meaningful error messages.

## 4. Example Scenario

1.  A developer goes to the **Configuration Page**.
2.  They set the following:
    *   **UI Label:** "Upload Profile Picture"
    *   **Storage Location:** `/var/www/my-app/uploads/avatars`
    *   **Allow Multiple Files:** Unchecked
    *   **File Prefix:** `user_`
3.  They save the configuration.
4.  In the main application, a user sees a file input labeled "Upload Profile Picture".
5.  The user selects a file named `photo.jpg`.
6.  They click "Upload".
7.  The backend receives the file, reads the configuration, prepends the prefix, and saves the file to `/var/www/my-app/uploads/avatars/user_photo.jpg`.

This prompt covers all necessary aspects to build the specified file upload widget.
