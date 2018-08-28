import re
from django.contrib import admin
import csv
from django import forms
from django.db import models
from django.http import HttpResponse
from .models import Characteristic, CharacteristicType, \
    Ingredient, State, Category, Group, Size, IngredientConversion, Pattern


admin.site.empty_value_display = ''

# Custom Forms


class ExportCsv:
    def export(self, request, queryset):
        meta = self.model._meta
        fields_names = [field.name for field in meta.fields]
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename={}.csv'.format(
            meta)
        writer = csv.writer(response)
        writer.writerow(fields_names)
        for obj in queryset:
            writer.writerow([getattr(obj, field)
                             for field in fields_names])
        return response

    def export_currentview(self, request, queryset):
        meta = self.model._meta
        fields = ['name', 'state', 'category', 'group', 'shelflife',
                  'alternates_str', 'finalspec_str', 'size_str',
                  'notes', 'isalternate', 'validated']
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename={}.csv'.format(
            meta)
        writer = csv.writer(response)
        writer.writerow(fields)
        for obj in queryset:
            writer.writerow([getattr(obj, field)
                             for field in fields])
        return response


class IngredientForm(forms.ModelForm):
    class Meta:
        widgets = {
            'isalternate': forms.CheckboxInput(check_test=lambda v: not (v is False)),
            'isfinalspec': forms.CheckboxInput(check_test=lambda v: not (v is False)),
            'validated': forms.CheckboxInput(check_test=lambda v: not (v is False)),
        }


class IngredientChangeListForm(forms.ModelForm):
    class Meta():
        widgets = {
            'state': forms.Select(attrs={'class': 'input-small', 'style': 'display: inline-block;'}),
            'category': forms.Select(attrs={'class': 'input-small', 'style': 'display: inline-block;'}),
            'group': forms.Select(attrs={'class': 'input-small', 'style': 'display: inline-block;'}),
            'get_alternates_str': forms.TextInput(attrs={'class': 'input-small', 'style': 'display: inline-block;'}),
            'get_finalspec_str': forms.TextInput(attrs={'class': 'input-small', 'style': 'display: inline-block;'}),
            'shelflife': forms.TextInput(attrs={'class': 'input-small', 'style': 'width: 20px;'}),
            'get_size_str': forms.TextInput(attrs={'class': 'input-small', 'style': 'display: inline-block;'}),
        }
        widgets.update(IngredientForm.Meta.widgets)

    def __init__(self, *args, **kwargs):
        """
        Override the form function in order to remove the add and change buttons
        beside the state and category
        """
        state_field = self.base_fields.get('state')
        if state_field:
            widget = state_field.widget
            widget.can_add_related = False
            widget.can_change_related = False

        catg_field = self.base_fields.get('category')
        if catg_field:
            widget = catg_field.widget
            widget.can_add_related = False
            widget.can_change_related = False
        super().__init__(*args, **kwargs)


class PatternForm(forms.ModelForm):
    class Meta():
        widgets = {'target_pattern': forms.TextInput(attrs={'class': 'input-small', 'style': 'width :400px;'}),
                   }


class CharacteristicForm(forms.ModelForm):
    class Meta():
        widgets = {
            'alternates': forms.TextInput(attrs={'class': 'input-small', 'style': 'width :500px;' 'display: inline-block;'}),
            'additional_info': forms.TextInput(attrs={'class': 'input-small', 'style': 'width :500px;' 'display: inline-block;'}),
        }


# Register your models here.


class CharacteristicAdmin(admin.ModelAdmin):
    list_display = ('name', 'type', 'alternates', "additional_info")
    search_fields = ['name']
    list_filter = ['type']
    list_editable = ('alternates', "additional_info")
    list_per_page = 50
    actions_on_top = True
    actions_on_bottom = False

    def get_changelist_form(self, request, **kwargs):
        return CharacteristicForm


class CharacteristicTypeAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ['name']
    list_per_page = 50
    actions_on_top = True
    actions_on_bottom = False


class IngredientAdmin(admin.ModelAdmin, ExportCsv):
    form = IngredientForm
    exclude = ('alternates_str', 'finalspec_str', 'size_str')
    list_display = ('name', 'state', 'category', 'group', 'shelflife',
                    'get_alternates_str', 'get_finalspec_str', 'get_size_str',
                    'notes', 'isalternate', 'validated')
    search_fields = ['name']
    list_filter = ['state', 'category', 'group',
                   'shelflife', 'isalternate', 'validated']
    list_editable = ('state', 'category', 'group',
                     'shelflife', 'isalternate', 'validated')
    filter_horizontal = ['alternates', 'finalspec', 'size_finalspec']
    list_select_related = ['state', 'category', 'group']
    actions_on_top = True
    actions_on_bottom = False
    list_per_page = 50
    actions = ['export', 'export_currentview']

    def get_alternates_str(self, obj):
        return obj.alternates_str
    get_alternates_str.short_description = 'Alternates'

    def get_finalspec_str(self, obj):
        return obj.finalspec_str
    get_finalspec_str.short_description = 'Finalspec'

    def get_size_str(self, obj):
        return obj.size_str
    get_size_str.short_description = 'Size_finalspec'

    def get_changelist_form(self, request, **kwargs):
        return IngredientChangeListForm

    def changelist_view(self, request, extra_context=None):
        """ Setting to display only records with isalternate as False """
        if len(request.GET) == 0:
            q = request.GET.copy()
            q['isalternate__exact'] = '0'
            request.GET = q
            request.META['QUERY_STRING'] = request.GET.urlencode()
        return super(IngredientAdmin, self).changelist_view(
            request, extra_context=extra_context)

    def get_form(self, request, obj=None, **kwargs):
        """
        Override the form function in order to remove the add and change buttons
        beside the foreign key pull-down menus
        """
        form = super(IngredientAdmin, self).get_form(request, obj, **kwargs)
        alt_field = form.base_fields.get('alternates')
        if alt_field:
            widget = alt_field.widget
            widget.can_add_related = False
            widget.can_change_related = False

        fs_field = form.base_fields.get('finalspec')
        if fs_field:
            widget = fs_field.widget
            widget.can_add_related = False
            widget.can_change_related = False
        return form

    def formfield_for_manytomany(self, db_field, request, **kwargs):
        """ Code to ignore selected ingredient form showing up in alternates list """
        if db_field.name == "alternates" or db_field.name == "finalspec":
            try:
                ing_id = int(
                    "".join(re.findall("ingredient/(\d+)/change", request.path)))
                kwargs["queryset"] = Ingredient.objects.exclude(id=ing_id)
                if db_field.name == "alternates":
                    # Excluding Ingredinets which are used as alternatvies from the
                    # Queryset and Including Ingredient alternative
                    ing_alternates = [i["id"] for i in Ingredient.objects.get(
                        id=ing_id).alternates.all().values("id")]
                    kwargs["queryset"] = kwargs["queryset"].exclude(
                        models.Q(isalternate=True) & ~models.Q(id__in=ing_alternates))
                elif db_field.name == "finalspec":
                    # Excluding Ingredinets which are used as alternatvies
                    kwargs["queryset"] = kwargs["queryset"].exclude(
                        models.Q(isalternate=True))
            except ValueError:
                pass
        return super(IngredientAdmin, self).formfield_for_manytomany(
            db_field, request, **kwargs)

    def formfield_for_dbfield(self, db_field, **kwargs):
        """
            Avoids querying db to get foreign key values in list_editable in each row
        """
        request = kwargs['request']
        formfield = super(IngredientAdmin, self).formfield_for_dbfield(
            db_field, **kwargs)
        if db_field.name in ["category", "state", "group"]:
            choices = getattr(request, '_%s_choices_cache' %
                              db_field.name, None)
            if choices is None:
                choices = [i for i in formfield.choices]
                setattr(request, '_%s_choices_cache' % db_field.name, choices)
            formfield.choices = choices
        return formfield


class StateAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ['name']
    list_per_page = 50
    actions_on_top = True
    actions_on_bottom = False


class CategoryAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ['name']
    list_per_page = 50
    save_on_top = True
    actions_on_top = True
    actions_on_bottom = False


class GroupAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ['name']
    list_per_page = 50
    actions_on_top = True
    actions_on_bottom = False


class SizeAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ['name']
    list_per_page = 50
    actions_on_top = True
    actions_on_bottom = False


class IngredientConversionAdmin(admin.ModelAdmin):
    list_display = ('ingredient', 'preparation', 'size',
                    'category', 'form', 'cup_per_unit')
    search_fields = ['ingredient']
    list_filter = ['ingredient', 'preparation', 'size', 'category', 'form']
    list_editable = ('cup_per_unit',)
    list_per_page = 50
    actions_on_top = True
    actions_on_bottom = False


class PatternAdmin(admin.ModelAdmin):
    list_display = ('pattern', 'pattern_type', 'target_pattern')
    search_fields = ['pattern']
    list_filter = ['pattern_type']
    list_editable = ("pattern_type", 'target_pattern')
    list_per_page = 50
    actions_on_top = True
    actions_on_bottom = False

    def get_changelist_form(self, request, **kwargs):
        return PatternForm


# Registering Models
admin.site.register(Characteristic, CharacteristicAdmin)
admin.site.register(CharacteristicType, CharacteristicTypeAdmin)
admin.site.register(Ingredient, IngredientAdmin)
admin.site.register(State, StateAdmin)
admin.site.register(Category, CategoryAdmin)
admin.site.register(Group, GroupAdmin)
admin.site.register(Size, SizeAdmin)
admin.site.register(IngredientConversion, IngredientConversionAdmin)
admin.site.register(Pattern, PatternAdmin)
