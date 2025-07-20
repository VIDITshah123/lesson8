const express = require('express');
const router = express.Router();
const fs = require('fs');
const path = require('path');

const configPath = path.join(__dirname, '..', 'widget-config.json');

router.get('/widget-config', (req, res) => {
    if (fs.existsSync(configPath)) {
        const config = fs.readFileSync(configPath, 'utf8');
        res.json(JSON.parse(config));
    } else {
        res.json({});
    }
});

router.post('/widget-config', (req, res) => {
    fs.writeFileSync(configPath, JSON.stringify(req.body, null, 2));
    res.json({ message: 'Configuration saved successfully' });
});

module.exports = router;
