from __future__ import annotations

import csv
import os
import re
import shutil
from typing import Any, Callable

from pydantic import ValidationError

from ...common import report_from_validation_error
from ...logger import logger
from ..models import CatalogItem
from .file_writer import FileWriter


class CSVWriter(FileWriter):
    """Writer to CSV table specialised on extracting parking information.

    Extracts the following CSV fields (added to the mapping):
      - parking_capacity        : общая вместимость (capacity.total)
      - parking_price_hour     : цена за час (из attributes / stop_factors)
      - parking_price_day      : цена за сутки
      - parking_price_month    : цена за месяц
      - parking_vehicle_types  : для каких типов ТС (например "Для грузовиков")
      - parking_guarded        : охраняемая (Yes/No)
      - parking_payment_methods: виды оплаты (наличные / картой / перевод и т.д.)

    The implementation performs a recursive search through the JSON `item` and
    through Pydantic model dict for occurrences of attributes, stop_factors and
    direct capacity objects.
    """

    @property
    def _complex_mapping(self) -> dict[str, Any]:
        # Keep original complex mapping for phones/emails etc. — unchanged
        return {
            'phone': 'Телефон', 'email': 'E-mail', 'website': 'Веб-сайт', 'instagram': 'Instagram',
            'twitter': 'Twitter', 'facebook': 'ВКонтакте', 'vkontakte': 'ВКонтакте', 'whatsapp': 'WhatsApp',
            'viber': 'Viber', 'telegram': 'Telegram', 'youtube': 'YouTube', 'skype': 'Skype'
        }

    @property
    def _data_mapping(self) -> dict[str, Any]:
        data_mapping = {
            'name': 'Наименование', 'description': 'Описание', 'rubrics': 'Рубрики',
            'address': 'Адрес', 'address_comment': 'Комментарий к адресу',
            'postcode': 'Почтовый индекс', 'living_area': 'Микрорайон', 'district': 'Район', 'city': 'Город',
            'district_area': 'Округ', 'region': 'Регион', 'country': 'Страна', 'schedule': 'Часы работы',
            'timezone': 'Часовой пояс', 'general_rating': 'Рейтинг', 'general_review_count': 'Количество отзывов'
        }

        # Expand complex mapping
        for k, v in self._complex_mapping.items():
            for n in range(1, self._options.csv.columns_per_entity + 1):
                data_mapping[f'{k}_{n}'] = f'{v} {n}'

        if not self._options.csv.add_rubrics:
            data_mapping.pop('rubrics', None)

        # Parking-specific fields
        data_mapping['parking_capacity'] = 'Парковка: вместимость'
        data_mapping['parking_price_hour'] = 'Парковка: цена/час'
        data_mapping['parking_price_day'] = 'Парковка: цена/сутки'
        data_mapping['parking_price_month'] = 'Парковка: цена/месяц'
        data_mapping['parking_vehicle_types'] = 'Парковка: типы транспорта'
        data_mapping['parking_guarded'] = 'Парковка: охраняемая'
        data_mapping['parking_payment_methods'] = 'Парковка: способы оплаты'

        return {
            **data_mapping,
            **{
                'point_lat': 'Широта',
                'point_lon': 'Долгота',
                'url': '2GIS URL',
            }
        }

    def _writerow(self, row: dict[str, Any]) -> None:
        """Write a `row` into CSV."""
        if self._options.verbose:
            logger.info('Парсинг [%d] > %s', self._wrote_count + 1, row.get('name'))

        try:
            self._writer.writerow(row)
        except Exception as e:
            logger.error('Ошибка во время записи: %s', e)

    def __enter__(self) -> CSVWriter:
        super().__enter__()
        self._writer = csv.DictWriter(self._file, self._data_mapping.keys())
        self._writer.writerow(self._data_mapping)  # Write header
        self._wrote_count = 0
        return self

    def __exit__(self, *exc_info) -> None:
        super().__exit__(*exc_info)
        if self._options.csv.remove_empty_columns:
            logger.info('Удаление пустых колонок CSV.')
            self._remove_empty_columns()
        if self._options.csv.remove_duplicates:
            logger.info('Удаление повторяющихся записей CSV.')
            self._remove_dublicates()

    def _remove_empty_columns(self) -> None:
        complex_columns = self._complex_mapping.keys()
        complex_columns_count = {c: 0 for c in self._data_mapping.keys() if
                                 re.match('|'.join(fr'^{x}_\d+$' for x in complex_columns), c)}

        # Looking for empty columns
        with self._open_file(self._file_path, 'r') as f_csv:
            csv_reader = csv.DictReader(f_csv, self._data_mapping.keys())  # type: ignore
            next(csv_reader, None)  # Skip header
            for row in csv.DictReader(f_csv, self._data_mapping.keys()):  # type: ignore
                for column_name in complex_columns_count.keys():
                    if row[column_name] != '':
                        complex_columns_count[column_name] += 1

        # Generate new data mapping
        new_data_mapping: dict[str, Any] = {}
        for k, v in self._data_mapping.items():
            if k in complex_columns_count:
                if complex_columns_count[k] > 0:
                    new_data_mapping[k] = v
            else:
                new_data_mapping[k] = v

        # Rename single complex column - remove postfix numbers
        for column in complex_columns:
            if f'{column}_1' in new_data_mapping and f'{column}_2' not in new_data_mapping:
                new_data_mapping[f'{column}_1'] = re.sub(r'\s+\d+$', '', new_data_mapping[f'{column}_1'])

        # Populate new csv
        tmp_csv_name = os.path.splitext(self._file_path)[0] + '.removed-columns.csv'

        with self._open_file(tmp_csv_name, 'w') as f_tmp_csv, \
                self._open_file(self._file_path, 'r') as f_csv:
            csv_writer = csv.DictWriter(f_tmp_csv, new_data_mapping.keys())  # type: ignore
            csv_reader = csv.DictReader(f_csv, self._data_mapping.keys())  # type: ignore
            csv_writer.writerow(new_data_mapping)  # Write new header
            next(csv_reader, None)  # Skip header

            for row in csv_reader:
                new_row = {k: v for k, v in row.items() if k in new_data_mapping}
                csv_writer.writerow(new_row)

        shutil.move(tmp_csv_name, self._file_path)

    def _remove_dublicates(self) -> None:
        tmp_csv_name = os.path.splitext(self._file_path)[0] + '.deduplicated.csv'
        with self._open_file(tmp_csv_name, 'w') as f_tmp_csv, \
                self._open_file(self._file_path, 'r') as f_csv:
            seen_records = set()
            for line in f_csv:
                if line in seen_records:
                    continue
                seen_records.add(line)
                f_tmp_csv.write(line)
        shutil.move(tmp_csv_name, self._file_path)

    def write(self, catalog_doc: Any) -> None:
        if not self._check_catalog_doc(catalog_doc):
            return
        row = self._extract_raw(catalog_doc)
        if row:
            self._writerow(row)
            self._wrote_count += 1

    # ------------------ helpers for parking extraction ------------------
    def _extract_price_from_name(self, name: str) -> str | None:
        """Normalize strings like "В сутки 100 ₽" -> "100 ₽" or "100".

        Returns normalized string or None.
        """
        if not name:
            return None
        name_norm = re.sub(r'[\s\u00A0]+', ' ', str(name)).strip()
        # search number with optional decimals and optional trailing currency
        m = re.search(r'([0-9\s\u00A0]+(?:[.,]\d+)?)\s*([₽$€A-Za-z]{1,4})?$', name_norm)
        if m:
            num = m.group(1).replace('\u00A0', '').replace(' ', '').replace(',', '.')
            cur = m.group(2) or ''
            if not cur and '₽' in name_norm:
                cur = '₽'
            return f'{num} {cur}'.strip()
        return name_norm if name_norm else None

    def _find_parking_values(self, obj: Any) -> dict[str, list[str]]:
        """Recursively walk `obj` (dict/list) and collect parking-related values.

        Returns dict with lists for keys: capacity, price_hour, price_day, price_month,
        vehicle_types, guarded, payment_methods.
        """
        found = {
            'capacity': [],
            'price_hour': [],
            'price_day': [],
            'price_month': [],
            'vehicle_types': [],
            'guarded': [],
            'payment_methods': [],
        }

        # tag patterns -> map to field names
        price_hour_patterns = (re.compile(p) for p in (r'parking_cost_parking_hour', r'car_parking_cost_parking_hour'))
        price_day_patterns = (re.compile(p) for p in (r'parking_cost_parking_day', r'car_parking_cost_parking_day'))
        price_month_patterns = (re.compile(p) for p in (r'parking_cost_parking_month', r'car_parking_cost_parking_month'))
        guarded_patterns = (re.compile(p) for p in (r'car_parking_guarded', r'car_parking_guarded_car_parking'))
        truck_patterns = (re.compile(p) for p in (r'car_parking_truck', r'car_parking_truck_car_parking'))
        payment_patterns = (re.compile(p) for p in (r'general_payment_type_', r'payment_type',))
        avg_price_pattern = re.compile(r'food_service_avg_price')  # sometimes parking prices also use similar tags

        def tag_matches(tag: str, patterns) -> bool:
            if not tag:
                return False
            t = tag.lower()
            for pat in patterns:
                if pat.search(t):
                    return True
            return False

        def walk(o: Any):
            if isinstance(o, dict):
                # capacity object
                if 'capacity' in o and isinstance(o['capacity'], dict):
                    total = o['capacity'].get('total')
                    if total:
                        found['capacity'].append(str(total))

                # attributes list (common place)
                if 'attributes' in o and isinstance(o['attributes'], list):
                    for a in o['attributes']:
                        if not isinstance(a, dict):
                            continue
                        tag = str(a.get('tag', '')).lower()
                        name = a.get('name') or a.get('value') or ''
                        # prices
                        if 'parking' in tag or 'parking_cost' in tag or 'car_parking' in tag:
                            # map specific tags
                            if 'month' in tag or 'parking_month' in tag or 'cost_parking_month' in tag:
                                found['price_month'].append(str(name))
                                continue
                            if 'day' in tag or 'parking_day' in tag or 'cost_parking_day' in tag:
                                found['price_day'].append(str(name))
                                continue
                            if 'hour' in tag or 'parking_hour' in tag or 'cost_parking_hour' in tag:
                                found['price_hour'].append(str(name))
                                continue
                        # guarded
                        if tag_matches(tag, guarded_patterns) or 'охраня' in (name or '').lower():
                            found['guarded'].append(str(name) or 'Охраняемая')
                        # truck / vehicle types
                        if tag_matches(tag, truck_patterns) or 'груз' in (name or '').lower():
                            found['vehicle_types'].append(str(name) or 'Для грузовиков')
                        # payment methods
                        if tag_matches(tag, payment_patterns) or 'налич' in (name or '').lower() or 'карта' in (name or '').lower():
                            found['payment_methods'].append(str(name))

                # stop_factors similar to attributes
                if 'stop_factors' in o and isinstance(o['stop_factors'], list):
                    for sf in o['stop_factors']:
                        if not isinstance(sf, dict):
                            continue
                        stag = str(sf.get('tag', '')).lower()
                        sname = sf.get('name') or ''
                        if 'parking' in stag or 'car_parking' in stag or 'parking_cost' in stag or 'food_service_avg_price' in stag:
                            # price-like
                            # decide day/month/hour by presence of words in name or tag
                            lname = str(sname)
                            if 'в месяц' in lname.lower() or 'month' in stag:
                                found['price_month'].append(lname)
                            elif 'в сутки' in lname.lower() or 'сут' in lname.lower() or 'day' in stag:
                                found['price_day'].append(lname)
                            elif 'в час' in lname.lower() or 'hour' in stag:
                                found['price_hour'].append(lname)
                            else:
                                # if tag explicit
                                if 'month' in stag:
                                    found['price_month'].append(lname)
                                elif 'day' in stag:
                                    found['price_day'].append(lname)
                                elif 'hour' in stag:
                                    found['price_hour'].append(lname)
                                else:
                                    # fallback: append to day
                                    found['price_day'].append(lname)
                        # payments or guarded or vehicle types
                        if 'guard' in stag or 'охраня' in (sf.get('name') or '').lower():
                            found['guarded'].append(sf.get('name') or 'Охраняемая')
                        if 'truck' in stag or 'груз' in (sf.get('name') or '').lower():
                            found['vehicle_types'].append(sf.get('name') or 'Для грузовиков')
                        if stag.startswith('general_payment_type') or 'налич' in (sf.get('name') or '').lower() or 'карта' in (sf.get('name') or '').lower():
                            found['payment_methods'].append(sf.get('name') or '')

                # direct tags like id/name pair
                tag = str(o.get('tag', '')).lower() if 'tag' in o else ''
                if tag:
                    # catch single-object cases
                    if 'capacity' in tag and 'total' in o:
                        found['capacity'].append(str(o.get('total')))

                # Recurse deeper
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                for item in o:
                    walk(item)
            else:
                return

        walk(obj)
        return found

    # -------------------------------------------------------------------------------

    def _extract_raw(self, catalog_doc: Any) -> dict[str, Any]:
        """Extract data from Catalog Item API JSON document with robust fallbacks.

        Uses both the pydantic model (catalog_item) and raw `item` dict as fallbacks.
        """
        data: dict[str, Any] = {k: None for k in self._data_mapping.keys()}

        # safe access to first item
        try:
            item = catalog_doc['result']['items'][0]
        except Exception as e:
            logger.error('Неверный формат catalog_doc: %s', e)
            return {}

        # Try to build pydantic model (we already did earlier in your code,
        # but keep it here to reuse in other parts)
        catalog_item = None
        try:
            catalog_item = CatalogItem(**item)
        except ValidationError as e:
            # Preserve existing behavior: log detailed validation errors and bail out
            errors = []
            errors_report = report_from_validation_error(e, item)
            for path, description in errors_report.items():
                arg = description.get('invalid_value')
                error_msg = description.get('error_message')
                errors.append(f'[*] Поле: {path}, значение: {arg}, ошибка: {error_msg}')
            error_str = 'Ошибка парсинга:\n' + '\n'.join(errors)
            error_str += '\nДокумент каталога: ' + str(catalog_doc)
            logger.error(error_str)
            return {}

        # --- Helper to get attribute from model then fallback to raw item dict ---
        def _model_or_raw(model_obj: Any, *model_attrs: str, raw_keys: list[str] | None = None):
            # try to traverse attributes on model_obj
            try:
                val = model_obj
                for a in model_attrs:
                    if val is None:
                        break
                    val = getattr(val, a, None)
                if val:
                    return val
            except Exception:
                pass
            # fallback to raw item keys
            if raw_keys:
                for k in raw_keys:
                    # support nested keys with dots
                    if '.' in k:
                        cur = item
                        ok = True
                        for part in k.split('.'):
                            if isinstance(cur, dict) and part in cur:
                                cur = cur[part]
                            else:
                                ok = False
                                break
                        if ok and cur:
                            return cur
                    else:
                        if k in item and item[k]:
                            return item[k]
            return None

        # --- Name / Description / Short name ---
        name_primary = _model_or_raw(catalog_item, 'name_ex', 'primary', raw_keys=['name', 'name_ex.primary'])
        name_extension = _model_or_raw(catalog_item, 'name_ex', 'extension', raw_keys=['name_ex.extension'])
        # if primary is an object (rare), try .primary attribute
        if hasattr(name_primary, 'primary') and not isinstance(name_primary, str):
            name_primary = getattr(name_primary, 'primary', None)

        if name_primary:
            data['name'] = name_primary
        else:
            # Fallback: try several common raw locations
            data['name'] = item.get('name') or (item.get('name_ex') and item.get('name_ex').get('primary')) or item.get('short_name')
            if not data['name']:
                logger.warning('Не удалось найти имя (name) в элементе: id=%s', item.get('id'))

        if name_extension:
            data['description'] = name_extension
        else:
            # fallback to raw item
            # try item['name_ex']['extension'] or item.get('description')
            desc = None
            if isinstance(item.get('name_ex'), dict):
                desc = item['name_ex'].get('extension')
            data['description'] = desc or item.get('description') or None

        # --- Address ---
        addr = _model_or_raw(catalog_item, 'address_name', raw_keys=['address_name', 'address.formatted', 'address'])
        data['address'] = addr if addr else None

        # --- Coordinates (point) ---
        # catalog_item.point may be None; handle safely
        try:
            if getattr(catalog_item, 'point', None):
                pt = catalog_item.point
                lat = getattr(pt, 'lat', None)
                lon = getattr(pt, 'lon', None)
            else:
                # fallback to raw item keys
                lat = item.get('lat') or (item.get('point') and item.get('point').get('lat'))
                lon = item.get('lon') or (item.get('point') and item.get('point').get('lon'))
        except Exception:
            lat = item.get('lat')
            lon = item.get('lon')

        if lat is not None:
            data['point_lat'] = lat
        if lon is not None:
            data['point_lon'] = lon

        # --- Address comment, postcode, timezone ---
        data['address_comment'] = _model_or_raw(catalog_item, 'address_comment', raw_keys=['address_comment', 'address.comment'])
        if getattr(catalog_item, 'address', None):
            try:
                data['postcode'] = getattr(catalog_item.address, 'postcode', None) or item.get('address', {}).get('postcode')
            except Exception:
                data['postcode'] = item.get('address', {}).get('postcode')
        else:
            # fallback
            addr_obj = item.get('address') if isinstance(item.get('address'), dict) else {}
            data['postcode'] = addr_obj.get('postcode') or None

        data['timezone'] = _model_or_raw(catalog_item, 'timezone', raw_keys=['timezone']) or None

        # --- Reviews (safe) ---
        try:
            if getattr(catalog_item, 'reviews', None):
                data['general_rating'] = getattr(catalog_item.reviews, 'general_rating', None)
                data['general_review_count'] = getattr(catalog_item.reviews, 'general_review_count', None)
            else:
                # raw fallback
                r = item.get('reviews') or {}
                if isinstance(r, dict):
                    data['general_rating'] = r.get('general_rating')
                    data['general_review_count'] = r.get('general_review_count')
        except Exception:
            # leave defaults None
            pass

        # --- Administrative divisions (adm_div) ---
        try:
            adm_divs = getattr(catalog_item, 'adm_div', None) or item.get('adm_div') or item.get('administrative_divisions') or []
            # adm_divs could be list of objects or dicts
            if isinstance(adm_divs, list):
                for div in adm_divs:
                    d_type = getattr(div, 'type', None) if not isinstance(div, dict) else div.get('type')
                    d_name = getattr(div, 'name', None) if not isinstance(div, dict) else div.get('name')
                    if d_type and d_name:
                        for t in ('country', 'region', 'district_area', 'city', 'district', 'living_area'):
                            if d_type == t:
                                data[t] = d_name
        except Exception:
            pass

        # --- URL ---
        data['url'] = _model_or_raw(catalog_item, 'url', raw_keys=['url']) or item.get('url') or None

        # --- Contacts (safe) ---
        try:
            contact_groups = getattr(catalog_item, 'contact_groups', None) or item.get('contact_groups') or []
            for contact_group in contact_groups:
                def append_contact(contact_type: str, priority_fields: list[str],
                                formatter: Callable[[str], str] | None = None) -> None:
                    contacts = []
                    # contact_group may be object or dict
                    if hasattr(contact_group, 'contacts'):
                        contacts = getattr(contact_group, 'contacts', []) or []
                    elif isinstance(contact_group, dict):
                        contacts = contact_group.get('contacts', []) or []

                    for i, contact in enumerate(contacts, 1):
                        contact_value = None
                        # contact may be pydantic model or dict
                        for field in priority_fields:
                            if isinstance(contact, dict):
                                if field in contact and contact[field]:
                                    contact_value = contact[field]
                                    break
                            else:
                                if hasattr(contact, field):
                                    contact_value = getattr(contact, field)
                                    break
                        if not contact_value:
                            continue
                        data_name = f'{contact_type}_{i}'
                        if data_name in data:
                            data[data_name] = formatter(contact_value) if formatter else contact_value
                            if self._options.csv.add_comments:
                                comment = contact.get('comment') if isinstance(contact, dict) else getattr(contact, 'comment', None)
                                if comment:
                                    data[data_name] = f'{data[data_name]} ({comment})'
                # apply known types
                for t in ['website', 'vkontakte', 'whatsapp', 'viber', 'telegram',
                        'instagram', 'facebook', 'twitter', 'youtube', 'skype']:
                    append_contact(t, ['url'])
                for t in ['email', 'skype']:
                    append_contact(t, ['value'])
                append_contact('phone', ['text', 'value'],
                            formatter=lambda x: re.sub(r'^\+7', '8', re.sub(r'[^0-9+]', '', x)))
        except Exception:
            # contacts are optional — ignore errors
            pass

        # --- The rest (parking extraction etc.) remains as in your class ---
        # (we keep the original parking-extraction code after this point)
        # For brevity here, call into your existing helpers:
        collected = {}
        try:
            collected = self._find_parking_values(getattr(catalog_item, '__dict__', {}))
        except Exception:
            collected = {}
        raw_found = self._find_parking_values(item)
        # combine lists
        combined = {}
        for k in ('capacity', 'price_hour', 'price_day', 'price_month', 'vehicle_types', 'guarded', 'payment_methods'):
            vals = []
            vals.extend(collected.get(k, []) if isinstance(collected.get(k, []), list) else [])
            vals.extend(raw_found.get(k, []) if isinstance(raw_found.get(k, []), list) else [])
            seen = set()
            uniq = []
            for v in vals:
                s = str(v).strip()
                if s and s not in seen:
                    uniq.append(s)
                    seen.add(s)
            combined[k] = uniq

        if combined['capacity']:
            data['parking_capacity'] = combined['capacity'][0]
        if combined['price_hour']:
            data['parking_price_hour'] = self._extract_price_from_name(combined['price_hour'][0])
        if combined['price_day']:
            data['parking_price_day'] = self._extract_price_from_name(combined['price_day'][0])
        if combined['price_month']:
            data['parking_price_month'] = self._extract_price_from_name(combined['price_month'][0])
        if combined['vehicle_types']:
            data['parking_vehicle_types'] = self._options.csv.join_char.join(combined['vehicle_types'])
        if combined['guarded']:
            data['parking_guarded'] = 'Да' if any(True for _ in combined['guarded']) else 'Нет'
        if combined['payment_methods']:
            data['parking_payment_methods'] = self._options.csv.join_char.join(combined['payment_methods'])

        # Schedule & Rubrics (safe)
        try:
            if getattr(catalog_item, 'schedule', None):
                data['schedule'] = catalog_item.schedule.to_str(self._options.csv.join_char,
                                                                self._options.csv.add_comments)
            elif isinstance(item.get('schedule'), dict):
                # fallback - represent schedule as raw JSON string or formatted summary
                data['schedule'] = None
        except Exception:
            pass

        if self._options.csv.add_rubrics:
            try:
                data['rubrics'] = self._options.csv.join_char.join(x.name for x in catalog_item.rubrics)
            except Exception:
                # fallback to raw rubrics if present
                raw_rub = item.get('rubrics') or []
                if isinstance(raw_rub, list):
                    try:
                        data['rubrics'] = self._options.csv.join_char.join(r.get('name') if isinstance(r, dict) else str(r) for r in raw_rub)
                    except Exception:
                        data['rubrics'] = None

        logger.debug(
            'Parsed parking fields: capacity=%s, hour=%s, day=%s, month=%s, vehicle=%s, guarded=%s, payment=%s',
            data.get('parking_capacity'), data.get('parking_price_hour'), data.get('parking_price_day'),
            data.get('parking_price_month'), data.get('parking_vehicle_types'), data.get('parking_guarded'),
            data.get('parking_payment_methods')
        )

        return data

