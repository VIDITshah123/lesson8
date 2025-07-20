import React, { useState, useEffect } from 'react';
import api from '../../services/api';

const FileUploadWidget = () => {
    const [config, setConfig] = useState({});
    const [files, setFiles] = useState([]);

    useEffect(() => {
        api.get('/widget-config').then(response => {
            setConfig(response.data);
        });
    }, []);

    const handleFileChange = (e) => {
        setFiles(e.target.files);
    };

    const handleUpload = () => {
        const formData = new FormData();
        for (let i = 0; i < files.length; i++) {
            formData.append('files', files[i]);
        }

        api.post('/upload', formData, {
            headers: {
                'Content-Type': 'multipart/form-data'
            }
        }).then(() => {
            alert('Files uploaded successfully!');
        });
    };

    return (
        <div>
            <h3>{config.uiLabel || 'File Upload'}</h3>
            <input type="file" multiple={config.allowMultiple} onChange={handleFileChange} />
            <button onClick={handleUpload}>Upload</button>
        </div>
    );
};

export default FileUploadWidget;
