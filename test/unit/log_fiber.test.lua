t = require('test_run').new()
logf = require('vshard.log_fiber')

box.cfg{log_level = 7}
log = logf.new({log_vshard_background = true})

-- Make sure that everything is forwarded to the log module
log:info('info_true')
t:grep_log('default', 'info_true')

log:warn('warn_true')
t:grep_log('default', 'warn_true')

log:debug('debug_true')
t:grep_log('default', 'debug_true')

log:error('error_true')
t:grep_log('default', 'error_true')

log:verbose('verbose_true')
t:grep_log('default', 'verbose_true')

log:cfg{log_vshard_background = false}
log.is_enabled

log:info('info_false')
t:grep_log('default', 'info_false')

log:warn('warn_false')
t:grep_log('default', 'warn_false')

log:debug('debug_false')
t:grep_log('default', 'debug_false')

log:error('error_false')
t:grep_log('default', 'error_false')

log:verbose('verbose_false')
t:grep_log('default', 'verbose_false')
