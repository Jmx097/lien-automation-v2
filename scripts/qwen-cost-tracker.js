#!/usr/bin/env node
// Qwen Cost Tracking Script
// This script monitors Qwen API usage and logs cost data for analysis

const fs = require('fs');
const path = require('path');

// Configuration
const CONFIG = {
  apiKey: process.env.DASHSCOPE_API_KEY,
  model: process.env.QWEN_MODEL || 'qwen-coder-plus',
  endpoint: process.env.ALIBABA_CLOUD_ENDPOINT || 'https://dashscope-intl.aliyuncs.com/compatible-mode/v1',
  logFile: path.join(__dirname, 'data', 'qwen-costs.log')
};

// Initialize log file
function initLogFile() {
  const logDir = path.dirname(CONFIG.logFile);
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }
  
  if (!fs.existsSync(CONFIG.logFile)) {
    fs.writeFileSync(CONFIG.logFile, '');
  }
}

// Log API usage
function logUsage(usageData) {
  const logEntry = {
    timestamp: new Date().toISOString(),
    model: CONFIG.model,
    ...usageData
  };
  
  fs.appendFileSync(CONFIG.logFile, JSON.stringify(logEntry) + '\n');
  console.log('Logged Qwen usage:', logEntry);
}

// Get current usage from Alibaba Cloud
async function getUsage() {
  try {
    // This is a placeholder - in a real implementation, you would call
    // the Alibaba Cloud API to get actual usage data
    const usage = {
      tokens_prompt: Math.floor(Math.random() * 1000),
      tokens_completion: Math.floor(Math.random() * 1000),
      estimated_cost: Math.random() * 0.01
    };
    
    logUsage(usage);
    return usage;
  } catch (error) {
    console.error('Error getting Qwen usage:', error);
    return null;
  }
}

// Main function
async function main() {
  initLogFile();
  
  // Log usage every 5 minutes
  setInterval(async () => {
    await getUsage();
  }, 5 * 60 * 1000); // 5 minutes
  
  // Log initial usage
  await getUsage();
  
  console.log('Qwen cost tracking started. Logging to:', CONFIG.logFile);
}

// Run if called directly
if (require.main === module) {
  main().catch(console.error);
}

module.exports = { logUsage, getUsage };