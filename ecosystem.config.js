/**
 * PM2 default production profile
 * Only the lien-scraper service is managed from this config.
 */
module.exports = {
  apps: [
    {
      name: 'lien-scraper',
      cwd: '.',
      script: 'docker-compose',
      args: 'up -d lien-scraper',
      instances: 1,
      exec_mode: 'fork',
      env: {
        NODE_ENV: 'production'
      },
      max_memory_restart: '1G',
      out_file: './logs/lien-automation-out.log',
      error_file: './logs/lien-automation-error.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      min_uptime: '60s',
      max_restarts: 10,
      restart_delay: 5000,
      watch: false
    }
  ]
};
