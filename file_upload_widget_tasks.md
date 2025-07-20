
# Tasks: Building the File Upload Widget

This document breaks down the development of the reusable file upload widget into a series of concrete tasks, based on the project prompt.

## Phase 1: Backend Development (Express.js)

**Objective:** Create the API endpoints to manage the widget's configuration and handle file uploads.

-   **Task 1.1: Create Configuration Endpoint**
    -   Create a new file: `backend/routes/widget-config.js`.
    -   Implement two routes:
        -   `GET /api/widget-config`: Reads and returns the current configuration from a JSON file (`widget-config.json`).
        -   `POST /api/widget-config`: Receives configuration data from the frontend and saves it to `widget-config.json`.
    -   Integrate this new route into the main `backend/app.js`.

-   **Task 1.2: Create File Upload Endpoint**
    -   Create a new file: `backend/routes/file-upload.js`.
    -   Implement a `POST /api/upload` endpoint.
    -   Use a middleware like `multer` to handle `multipart/form-data`.
    -   The endpoint logic must:
        1.  Read the `widget-config.json` file to get the storage location and prefix.
        2.  Ensure the storage directory exists (create it if it doesn't).
        3.  Apply the configured file name prefix.
        4.  Save the uploaded file(s) to the correct directory.
        5.  Return a success or error response.
    -   Integrate this new route into `backend/app.js`.

## Phase 2: Frontend Development (React)

**Objective:** Build the UI components for configuring the widget and for performing the file upload.

-   **Task 2.1: Create Configuration Page Component**
    -   Create a new component file: `frontend/src/components/fileupload/FileUploadConfig.js`.
    -   Build a form with input fields for:
        -   UI Label (text)
        -   Upload Storage Location (text)
        -   Allow Multiple Files (checkbox)
        -   File Name Prefix (text)
    -   On component mount, fetch the current settings from `GET /api/widget-config` and populate the form.
    -   Implement a `handleSubmit` function that sends the form data to `POST /api/widget-config`.

-   **Task 2.2: Create the File Upload Widget Component**
    -   Create a new component file: `frontend/src/components/fileupload/FileUploadWidget.js`.
    -   On component mount, fetch the widget's configuration from `GET /api/widget-config`.
    -   Dynamically render the component based on the fetched configuration:
        -   Display the configured UI Label.
        -   Set the `multiple` attribute on the `<input type="file" />` based on the configuration.
    -   Implement state management for selected files.
    -   Create an `handleUpload` function that:
        -   Creates a `FormData` object.
        -   Appends the selected file(s).
        -   Sends the data to the `POST /api/upload` endpoint.

-   **Task 2.3: Integrate New Components into the App**
    -   Add a new route in `frontend/src/App.js` to render the `FileUploadConfig` page (e.g., at `/admin/file-upload-settings`).
    -   Add a link to this new page in the sidebar (`frontend/src/components/common/Sidebar.js`) for easy access.
    -   Place the `FileUploadWidget` component onto an existing page (e.g., the `Dashboard`) to demonstrate its use.

## Phase 3: Finalization & Testing

**Objective:** Ensure the feature is working end-to-end and is robust.

-   **Task 3.1: End-to-End Testing**
    -   Navigate to the configuration page and save a new configuration.
    -   Go to the page containing the widget and verify the UI matches the new configuration.
    -   Test single file upload.
    -   Re-configure to allow multiple files and test multiple file uploads.
    -   Verify that files are correctly saved with the specified prefix in the correct server directory.
    -   Test edge cases (e.g., not selecting a file, saving an empty configuration).
