type Scalar = string | number | boolean | null

type GenericSerializable = Scalar | undefined | void | GenericSerializable[] | { [key: string]: GenericSerializable }

export type Data = GenericSerializable

export type Validator<Out = Data> = {
	parse: (input: any) => Out
}

export type DeepPartial<T> = T extends object ? {
	[P in keyof T]?: DeepPartial<T[P]>
} : T