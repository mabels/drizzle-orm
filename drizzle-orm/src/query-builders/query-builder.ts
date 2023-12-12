import { entityKind } from '~/entity.ts';
import type { SQL, SQLWrapper } from '~/sql/index.ts';

export abstract class TypedQueryBuilder<TSelection, TItem, TResult = unknown> implements SQLWrapper {
	static readonly [entityKind]: string = 'TypedQueryBuilder';

	declare _: {
		selectedFields: TSelection;
		result: TResult;
		item: TItem;
	};

	/** @internal */
	getSelectedFields(): TSelection {
		return this._.selectedFields;
	}

	abstract getSQL(): SQL;
}
