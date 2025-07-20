const express = require('express');
const router = express.Router();
const multer = require('multer');
const fs = require('fs');
const path = require('path');

const configPath = path.join(__dirname, '..', 'widget-config.json');

const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
        const dest = config.uploadLocation || 'uploads/';
        if (!fs.existsSync(dest)) {
            fs.mkdirSync(dest, { recursive: true });
        }
        cb(null, dest);
    },
    filename: function (req, file, cb) {
        const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
        const prefix = config.filePrefix || '';
        cb(null, prefix + file.originalname);
    }
});

const upload = multer({ storage: storage });

router.post('/upload', upload.array('files'), (req, res) => {
    res.json({ message: 'Files uploaded successfully' });
});

module.exports = router;
