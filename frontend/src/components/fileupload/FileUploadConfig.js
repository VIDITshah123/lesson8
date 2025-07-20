import React, { useState, useEffect } from 'react';
import api from '../../services/api';

const FileUploadConfig = () => {
    const [config, setConfig] = useState({
        uiLabel: '',
        uploadLocation: '',
        allowMultiple: false,
        filePrefix: ''
    });

    useEffect(() => {
        api.get('/widget-config').then(response => {
            setConfig(response.data);
        });
    }, []);

    const handleChange = (e) => {
        const { name, value, type, checked } = e.target;
        setConfig(prevConfig => ({
            ...prevConfig,
            [name]: type === 'checkbox' ? checked : value
        }));
    };

    const handleSubmit = (e) => {
        e.preventDefault();
        api.post('/widget-config', config).then(() => {
            alert('Configuration saved!');
        });
    };

    return (
        <form onSubmit={handleSubmit}>
            <h2>File Upload Widget Configuration</h2>
            <div>
                <label>UI Label</label>
                <input type="text" name="uiLabel" value={config.uiLabel} onChange={handleChange} required />
            </div>
            <div>
                <label>Upload Storage Location</label>
                <input type="text" name="uploadLocation" value={config.uploadLocation} onChange={handleChange} required />
            </div>
            <div>
                <label>
                    <input type="checkbox" name="allowMultiple" checked={config.allowMultiple} onChange={handleChange} />
                    Allow Multiple Files
                </label>
            </div>
            <div>
                <label>File Name Prefix</label>
                <input type="text" name="filePrefix" value={config.filePrefix} onChange={handleChange} />
            </div>
            <button type="submit">Save Configuration</button>
        </form>
    );
};

export default FileUploadConfig;
