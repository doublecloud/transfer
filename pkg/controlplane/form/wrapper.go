package form

import (
	"context"

	dynamicform_api "github.com/doublecloud/transfer/cloud/bitbucket/private-api/yandex/cloud/priv/dynamicform/v1"
	"github.com/doublecloud/transfer/cloud/dataplatform/api/options"
	"github.com/doublecloud/transfer/cloud/dataplatform/dynamicform"
	"github.com/doublecloud/transfer/pkg/config/env"
	i18nkeys "github.com/doublecloud/transfer/translations"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type DefaultPropertyProvider struct {
}

func NewDefaultPropertyProvider() *DefaultPropertyProvider {
	return &DefaultPropertyProvider{}
}

func (p *DefaultPropertyProvider) visibilityContains(visibilities []options.Visibility, visibilityToCheck options.Visibility) bool {
	for _, visibility := range visibilities {
		if visibility == visibilityToCheck {
			return true
		}
	}
	return false
}

func (p *DefaultPropertyProvider) GetVisible(visibility []options.Visibility, ctx context.Context, locale string, protoItem protoreflect.Descriptor, defaultVisible bool) (bool, error) {
	// If there no flags - field visible
	if len(visibility) == 0 {
		return true, nil
	}

	// If visibility contains "visible" flag - field always visible;
	// "Visible" flag beats other flags;
	if p.visibilityContains(visibility, options.Visibility_VISIBILITY_VISIBLE) {
		return true, nil
	}
	// If visibility contains "hidden" flag - field always hidden;
	// "Hidden" flag beats "visible" flag and others;
	if p.visibilityContains(visibility, options.Visibility_VISIBILITY_HIDDEN) {
		return false, nil
	}

	// Checking environments flags
	switch env.Get() {
	case env.EnvironmentInternal:
		return p.visibilityContains(visibility, options.Visibility_VISIBILITY_IN), nil
	case env.EnvironmentRU:
		return p.visibilityContains(visibility, options.Visibility_VISIBILITY_CLOUD) || p.visibilityContains(visibility, options.Visibility_VISIBILITY_RU), nil
	case env.EnvironmentKZ:
		return p.visibilityContains(visibility, options.Visibility_VISIBILITY_CLOUD) || p.visibilityContains(visibility, options.Visibility_VISIBILITY_KZ), nil
	case env.EnvironmentAWS:
		return p.visibilityContains(visibility, options.Visibility_VISIBILITY_DOUBLECLOUD), nil
	case env.EnvironmentNebius:
		return p.visibilityContains(visibility, options.Visibility_VISIBILITY_NEBIUS), nil
	default:
		return true, nil
	}
}

// TODO Rename after move out dynamic forms
type DTLinkProvider interface {
	GetLink(link string, folderID string) (*dynamicform_api.Link, error)
}

type DefaultLinkProvider struct {
	folderID string
	entity   interface{}
}

func NewDefaultLinkProvider(folderID string, entity interface{}) *DefaultLinkProvider {
	return &DefaultLinkProvider{
		folderID: folderID,
		entity:   entity,
	}
}

func (l *DefaultLinkProvider) GetLink(
	link string,
	ctx context.Context,
	locale string,
	protoItem protoreflect.Descriptor,
) (*dynamicform_api.Link, error) {
	if l.entity == nil {
		return nil, nil
	}

	dtLinkProvider, ok := l.entity.(DTLinkProvider)
	if !ok {
		return nil, nil
	}

	return dtLinkProvider.GetLink(link, l.folderID)
}

type DefaultTextProvider struct {
}

func NewDefaultTextProvider() *DefaultTextProvider {
	return &DefaultTextProvider{}
}

func (t *DefaultTextProvider) GetTitle(
	key string,
	ctx context.Context,
	locale string,
	protoItem protoreflect.Descriptor,
	defaultText string,
) (string, error) {
	return i18nkeys.LocalizedTitle(locale, key, defaultText), nil
}

func (t *DefaultTextProvider) GetUsage(
	key string,
	ctx context.Context,
	locale string,
	protoItem protoreflect.Descriptor,
	defaultText string,
) (string, error) {
	return i18nkeys.LocalizedUsage(locale, key, defaultText), nil
}

func (t *DefaultTextProvider) GetItemLabel(key string,
	ctx context.Context,
	locale string,
	protoItem protoreflect.Descriptor,
	defaultText string,
) (string, error) {
	return i18nkeys.LocalizedItemLabel(locale, key, defaultText), nil
}

func (t *DefaultTextProvider) GetPatternError(
	key string,
	ctx context.Context,
	locale string,
	protoItem protoreflect.Descriptor,
	defaultText string,
) (string, error) {
	return i18nkeys.LocalizedPatternError(locale, key, defaultText), nil
}

type DynamicFormCustomAction func(
	ctx context.Context,
	locale string,
	protoItem protoreflect.Descriptor,
	item *dynamicform_api.Item,
) (*dynamicform_api.Item, error)

type DefaultCustomActionProvider struct {
	actions map[string]DynamicFormCustomAction
}

func NewDefaultCustomActionProvider(actions map[string]DynamicFormCustomAction) *DefaultCustomActionProvider {
	return &DefaultCustomActionProvider{
		actions: actions,
	}
}

func (a *DefaultCustomActionProvider) CustomAction(
	customAction string,
	ctx context.Context,
	locale string,
	protoItem protoreflect.Descriptor,
	item *dynamicform_api.Item,
) (*dynamicform_api.Item, error) {
	if a.actions == nil {
		return item, nil
	}

	customActionFunc := a.actions[customAction]
	if customActionFunc == nil {
		return item, nil
	}

	return customActionFunc(ctx, locale, protoItem, item)
}

func NewDefaultDynamicForm(
	formType dynamicform.FormType,
	formProto protoreflect.MessageDescriptor,
	folderID string,
	entity interface{},
	actions map[string]DynamicFormCustomAction,
) *dynamicform.DynamicForm {
	return dynamicform.NewDynamicForm(formType, formProto).
		AddPropertyProvider(NewDefaultPropertyProvider()).
		AddLinkProvider(NewDefaultLinkProvider(folderID, entity)).
		AddTextProvider(NewDefaultTextProvider()).
		AddCustomActionProvider(NewDefaultCustomActionProvider(actions))
}

func NoAction() DynamicFormCustomAction {
	return func(
		ctx context.Context,
		locale string,
		protoItem protoreflect.Descriptor,
		item *dynamicform_api.Item,
	) (*dynamicform_api.Item, error) {
		return item, nil
	}
}

func HideFieldAction(hide bool) DynamicFormCustomAction {
	return func(
		ctx context.Context,
		locale string,
		protoItem protoreflect.Descriptor,
		item *dynamicform_api.Item,
	) (*dynamicform_api.Item, error) {
		if hide {
			return nil, nil
		} else {
			return item, nil
		}
	}
}

func DisableFieldAction(disable bool) DynamicFormCustomAction {
	return func(
		ctx context.Context,
		locale string,
		protoItem protoreflect.Descriptor,
		item *dynamicform_api.Item,
	) (*dynamicform_api.Item, error) {
		item.ViewSpec.Disabled = disable
		return item, nil
	}
}

// HideWithReturnFieldAction like HideFieldAction, but field goes back to API
func HideWithReturnFieldAction(hidden bool) DynamicFormCustomAction {
	return func(
		ctx context.Context,
		locale string,
		protoItem protoreflect.Descriptor,
		item *dynamicform_api.Item,
	) (*dynamicform_api.Item, error) {
		item.ViewSpec.Type = ""
		item.ViewSpec.Hidden = hidden
		return item, nil
	}
}

func FormItemToEnum(item *dynamicform_api.Item, values []string, labels map[string]string) *dynamicform_api.Item {
	item.ViewSpec.Type = "select"
	item.Enum = values
	item.Description = labels
	return item
}

func EmptyEnumAction() DynamicFormCustomAction {
	return func(
		ctx context.Context,
		locale string,
		protoItem protoreflect.Descriptor,
		item *dynamicform_api.Item,
	) (*dynamicform_api.Item, error) {
		return FormItemToEnum(item, nil, nil), nil
	}
}
