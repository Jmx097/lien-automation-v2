#!/bin/bash
# PM2 management script for Mission Control

case "$1" in
  start)
    echo "Starting Mission Control with PM2..."
    cd /root/lien-automation-v2
    pm2 start pm2-mission-control.config.js
    ;;
  stop)
    echo "Stopping Mission Control..."
    pm2 stop mission-control
    ;;
  restart)
    echo "Restarting Mission Control..."
    pm2 restart mission-control
    ;;
  status)
    echo "Mission Control status:"
    pm2 list
    ;;
  logs)
    echo "Mission Control logs:"
    pm2 logs mission-control
    ;;
  monit)
    echo "Monitoring Mission Control..."
    pm2 monit
    ;;
  save)
    echo "Saving PM2 configuration..."
    pm2 save
    ;;
  startup)
    echo "Setting up PM2 startup..."
    pm2 startup
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs|monit|save|startup}"
    echo ""
    echo "Commands:"
    echo "  start   - Start Mission Control with PM2"
    echo "  stop    - Stop Mission Control"
    echo "  restart - Restart Mission Control"
    echo "  status  - Show PM2 process status"
    echo "  logs    - Show Mission Control logs"
    echo "  monit   - Monitor processes"
    echo "  save    - Save current PM2 configuration"
    echo "  startup - Set up PM2 to start on boot"
    exit 1
    ;;
esac