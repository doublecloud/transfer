package i18nkeys

import (
	"embed"
	"encoding/json"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

//go:embed i18n/data-transfer.*/ru.json
//go:embed i18n/data-transfer.*/en.json
var keyFileFS embed.FS

var keysetPrefix = "data-transfer"

var KeysInfo map[string]*KeyInfo

type FormTranslation struct {
	Ru string
	En string
}

type KeyInfo struct {
	Title        *FormTranslation
	Usage        *FormTranslation
	ItemLabel    *FormTranslation
	PatternError *FormTranslation
}

func LocalizedTitle(locale string, key string, defaultText string) string {
	var translation *FormTranslation
	if info, ok := KeysInfo[key]; ok {
		translation = info.Title
	}
	return localizeTranslation(translation, locale, defaultText)
}

func LocalizedUsage(locale string, key string, defaultText string) string {
	var translation *FormTranslation
	if info, ok := KeysInfo[key]; ok {
		translation = info.Usage
	}
	return localizeTranslation(translation, locale, defaultText)
}

func LocalizedItemLabel(locale string, key string, defaultText string) string {
	var translation *FormTranslation
	if info, ok := KeysInfo[key]; ok {
		translation = info.ItemLabel
	}
	return localizeTranslation(translation, locale, defaultText)
}

func LocalizedPatternError(locale string, key string, defaultText string) string {
	var translation *FormTranslation
	if info, ok := KeysInfo[key]; ok {
		translation = info.PatternError
	}
	return localizeTranslation(translation, locale, defaultText)
}

func localizeTranslation(translation *FormTranslation, locale string, defaultText string) string {
	if translation == nil {
		return defaultText
	}

	if locale == "ru" && translation.Ru != "" {
		return translation.Ru
	}
	if translation.En != "" {
		return translation.En
	}

	return defaultText
}

func processKey(translationKey string, val string, locale string) error {
	keyPath := strings.Split(translationKey, ".")
	if len(keyPath) < 2 {
		return xerrors.Errorf("Wrong format of key '%v'", translationKey)
	}

	key := strings.Join(keyPath[:len(keyPath)-1], ".")
	var keyInfo *KeyInfo
	if info, ok := KeysInfo[key]; !ok {
		keyInfo = new(KeyInfo)
		KeysInfo[key] = keyInfo
	} else {
		keyInfo = info
	}

	field := keyPath[len(keyPath)-1]
	var translation *FormTranslation
	switch field {
	case "title":
		translation = keyInfo.Title
		if translation == nil {
			translation = new(FormTranslation)
			keyInfo.Title = translation
		}
	case "usage":
		translation = keyInfo.Usage
		if translation == nil {
			translation = new(FormTranslation)
			keyInfo.Usage = translation
		}
	case "array_item_label":
		translation = keyInfo.ItemLabel
		if translation == nil {
			translation = new(FormTranslation)
			keyInfo.ItemLabel = translation
		}
	case "pattern_error":
		translation = keyInfo.PatternError
		if translation == nil {
			translation = new(FormTranslation)
			keyInfo.PatternError = translation
		}
	default:
		return xerrors.Errorf("Wrong type of field '%s'", field)
	}

	switch locale {
	case "ru":
		if translation.Ru != "" {
			return xerrors.Errorf("Repeated locale '%s' of field '%s'", locale, field)
		}
		translation.Ru = val
	case "en":
		if translation.En != "" {
			return xerrors.Errorf("Repeated locale '%s' of field '%s'", locale, field)
		}
		translation.En = val
	default:
		return xerrors.Errorf("Wrong locale '%s'", locale)
	}
	return nil
}
func init() {
	errors := util.NewErrs()
	KeysInfo = make(map[string]*KeyInfo)
	err := fs.WalkDir(keyFileFS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			errors = util.AppendErr(errors,
				xerrors.Errorf("Error while walking key files dir tree: %w", err))
			return nil
		}

		if d.Type().IsDir() {
			return nil
		}

		lang := d.Name()
		lang = lang[:len(lang)-len(filepath.Ext(lang))]

		fileContents, err := keyFileFS.ReadFile(path)
		if err != nil {
			errors = util.AppendErr(errors,
				xerrors.Errorf("Failed to get key file '%s' contents: %w", path, err))
			return nil
		}
		translationKeys := make(map[string]string, 0)
		err = json.Unmarshal(fileContents, &translationKeys)
		if err != nil {
			errors = util.AppendErr(errors,
				xerrors.Errorf("Failed to unmarshal file '%s' contents: %w", path, err))
			return nil
		}

		for translationKey, val := range translationKeys {
			if val != "" {
				err = processKey(translationKey, val, lang)
				if err != nil {
					errors = util.AppendErr(errors,
						xerrors.Errorf("Failed to process key '%s' of file '%s': %w", translationKey, path, err))
				}
			}
		}
		return nil
	})

	if err != nil {
		errors = util.AppendErr(errors, err)
	}

	for _, err := range errors {
		logger.Log.Warn("Error while processing translation files", log.Error(err))
	}
}
