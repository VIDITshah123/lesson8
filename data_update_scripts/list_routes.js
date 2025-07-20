const express = require('express');
const app = require('./backend/app');

// Function to print all registered routes
function printRoutes() {
  console.log('All registered routes in the application:');
  console.log('----------------------------------------');
  
  const routes = [];
  
  // Function to extract routes from a layer
  function extractRoutes(layer, basePath = '') {
    if (layer.route) {
      const methods = Object.keys(layer.route.methods)
        .filter(method => layer.route.methods[method])
        .map(method => method.toUpperCase());
      
      routes.push({
        path: basePath + layer.route.path,
        methods: methods.join(', ')
      });
    } else if (layer.name === 'router' && layer.handle.stack) {
      // It's a router middleware
      const routerPath = layer.regexp.toString().includes('^\\/api')
        ? layer.regexp.toString().split('^\\/api')[1].split('\\/?')[0].replace(/\\/g, '')
        : '';
      
      const newBasePath = basePath + (routerPath ? `/${routerPath}` : '');
      
      layer.handle.stack.forEach(stackItem => {
        extractRoutes(stackItem, newBasePath);
      });
    }
  }
  
  // Extract routes from the main router
  app._router.stack.forEach(layer => {
    extractRoutes(layer);
  });
  
  // Print routes in a sorted way
  routes.sort((a, b) => a.path.localeCompare(b.path));
  
  routes.forEach(route => {
    console.log(`[${route.methods}] ${route.path}`);
  });
}

// Print all routes
printRoutes();
