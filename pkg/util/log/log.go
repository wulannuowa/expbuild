package log

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
}

func Debugf(format string, args ...interface{}) {
	log.Debug().Msgf(format, args...)
}

func Infof(format string, args ...interface{}) {
	log.Info().Msgf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	log.Error().Msgf(format, args...)
}
