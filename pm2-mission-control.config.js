module.exports = {
  apps: [
    {
      name: 'mission-control',
      cwd: './mission-control',
      script: 'npm',
      args: 'start',
      instances: 1,
      exec_mode: 'fork',
      env: {
        NODE_ENV: 'production',
        PORT: '4000'
      },
      max_memory_restart: '1G',
      node_args: '--max-old-space-size=1024',
      out_file: './logs/mission-control-out.log',
      error_file: './logs/mission-control-error.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      min_uptime: '60s',
      max_restarts: 10,
      restart_delay: 5000,
      watch: false
    }
  ]
};